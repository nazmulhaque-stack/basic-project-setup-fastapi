"""User Service: Clean domain orchestration with direct Redis caching, Transactional Outbox, and Elasticsearch search."""

import asyncio
from datetime import datetime, timezone
import logging
from typing import Annotated, Any, Dict, List, Optional
import uuid

from fastapi import Depends, Request

from app.api.deps import CryptoDep, ElasticsearchDep, RedisDep, SessionDep
from app.core.cache_interface import ICacheService, NullCache
from app.core.config import Settings, get_settings
from app.core.elasticsearch import IElasticsearchService, NullElasticsearch
from app.core.exceptions import ConflictError, DecryptionError, NotFoundError
from app.core.security import CryptoService
from app.db.pool import UnitOfWork
from app.models.outbox import OutboxEvent
from app.models.user import User, UserAccount
from app.repositories.outbox import OutboxRepository
from app.repositories.user import UserRepository
from app.schemas.base import decode_cursor, encode_cursor
from app.schemas.user import (
    UserCreate,
    UserPage,
    UserRead,
    UserUpdate,
)
from app.services.cache import DynamicRedisCache

logger = logging.getLogger(__name__)


class UserService:
    """User domain service: PII cryptography, Redis L1 caching, Transactional Outbox, and Elasticsearch search."""

    def __init__(
        self,
        db: UnitOfWork,
        cache: Optional[ICacheService] = None,
        crypto: Optional[CryptoService] = None,
        elasticsearch: Optional[IElasticsearchService] = None,
        outbox_worker: Optional[Any] = None,
        **_: Any,
    ) -> None:
        self.db = db
        self.repo = UserRepository(db)
        self.outbox_repo = OutboxRepository(db)
        self.cache: ICacheService = cache or NullCache()
        self.crypto: Optional[CryptoService] = crypto
        self.es: IElasticsearchService = elasticsearch or NullElasticsearch()
        self.outbox_worker = outbox_worker

    
    # --------------------------------------------------------------------------
    # 1. CREATE USER (POST /users)
    # --------------------------------------------------------------------------
    async def create_user(self, payload: UserCreate) -> UserRead:
        """Create a user with PII encryption, HMAC blind index, Redis pre-warming, and Outbox ES sync."""
        # Uniqueness check for email
        existing_email = await self.repo.get_by_email(str(payload.email))
        if existing_email:
            raise ConflictError("A user with this email address already exists.")

        user_id = uuid.uuid4()
        phone_encrypted = payload.phone
        phone_blind_index = payload.phone

        # Cryptographic phone encryption & HMAC blind index
        if self.crypto is not None:
            blind_index = self.crypto.blind_index(payload.phone)
            existing_phone = await self.repo.get_by_phone_blind_index(blind_index)
            if existing_phone:
                raise ConflictError("A user with this phone number already exists.")

            aad = f"users.phone:{user_id}".encode("utf-8")
            phone_encrypted = self.crypto.encrypt(payload.phone, aad=aad)
            phone_blind_index = blind_index

        user = User(
            id=user_id,
            name=payload.name,
            email=str(payload.email),
            role=payload.role,
            department=payload.department,
            department_id=None,
            role_id=None,
            phone_encrypted=phone_encrypted,
            phone_blind_index=phone_blind_index,
            created_at=datetime.now(timezone.utc),
        )

        created = await self.repo.create(user)

        # Optional initial accounts creation
        if payload.accounts:
            for acc_payload in payload.accounts:
                account_model = UserAccount(
                    id=uuid.uuid4(),
                    user_id=user_id,
                    account_number=acc_payload.account_number,
                    account_type=acc_payload.account_type,
                    balance_cents=acc_payload.balance_cents,
                    currency=acc_payload.currency,
                    status=acc_payload.status,
                    created_at=datetime.now(timezone.utc),
                    updated_at=datetime.now(timezone.utc),
                )
                await self.repo.create_account(account_model)

        read_model = self._to_read_schema(created)

        # Stage Outbox Event BEFORE DB commit (Atomic consistency)
        outbox_event = await self.outbox_repo.create_event(
            aggregate_type="user",
            aggregate_id=str(created.id),
            event_type="CREATE",
            payload=self._build_user_es_doc(created),
        )

        # Cache synchronization:
        #   - Invalidate list cache (O(1) version bump) to prevent stale paginated list reads.
        #   - Pre-warm item cache (write-through) for instant sub-millisecond subsequent reads.
        await self.cache.invalidate_lists()
        await self.cache.set_item(str(created.id), read_model.model_dump(mode="json"))

        # Atomic commit
        await self.db.commit()

        # Dispatch Outbox synchronization to Elasticsearch
        await self._dispatch_outbox_sync(outbox_event)

        return read_model

    # --------------------------------------------------------------------------
    # 2. GET USER BY ID (GET /users/{user_id})
    # --------------------------------------------------------------------------
    async def get_user_by_id(self, user_id: uuid.UUID) -> UserRead:
        """Fetch user by ID with 3-tier Self-Healing Fallback (Redis -> ES -> PostgreSQL)."""
        # Level 1: Redis L1 Cache-Aside
        try:
            cached = await self.cache.get_item(str(user_id))
            if cached is not None:
                if isinstance(cached, dict) and cached.get("__not_found__"):
                    raise NotFoundError(f"User with ID '{user_id}' not found.")
                return UserRead(**cached)
        except NotFoundError:
            raise
        except Exception as exc:
            logger.warning("Redis read error in get_user_by_id: %s", exc)

        # Level 2: Elasticsearch L2
        try:
            es_doc = await self.es.get_document(index="user", id_=str(user_id))
            if es_doc is not None:
                user_read = UserRead(
                    id=es_doc["id"],
                    name=es_doc.get("name", ""),
                    email=es_doc.get("email", ""),
                    role=es_doc.get("role", "user"),
                    department=es_doc.get("department"),
                    phone_masked=None,
                    created_at=datetime.fromisoformat(es_doc["created_at"]) if es_doc.get("created_at") else None,
                )
                try:
                    await self.cache.set_item(str(user_id), user_read.model_dump(mode="json"))
                except Exception:
                    pass
                return user_read
        except Exception as exc:
            logger.warning("Elasticsearch read error in get_user_by_id: %s", exc)

        # Level 3: Fallback to PostgreSQL (Authoritative Store)
        user = await self.repo.get(user_id)
        if user is None:
            try:
                await self.cache.set_negative_cache(str(user_id))
            except Exception:
                pass
            raise NotFoundError(f"User with ID '{user_id}' not found.")

        read_model = self._to_read_schema(user)

        # Self-Heal: Backfill both Redis and Elasticsearch
        try:
            await self.cache.set_item(str(user.id), read_model.model_dump(mode="json"))
        except Exception as exc:
            logger.warning("Failed to backfill Redis: %s", exc)

        try:
            await self.es.index_document(index="user", id_=str(user_id), document=self._build_user_es_doc(user))
        except Exception as exc:
            logger.warning("Failed to backfill Elasticsearch: %s", exc)

        return read_model

    # --------------------------------------------------------------------------
    # 3. LIST USERS (GET /users - Keyset Cursor Pagination)
    # --------------------------------------------------------------------------
    async def list_users(
        self,
        limit: int = 50,
        cursor: Optional[str] = None,
        role: Optional[str] = None,
    ) -> UserPage:
        """List users using keyset (cursor) B-Tree pagination and Redis list caching."""
        safe_limit = min(100, max(1, limit))
        filter_params = {"limit": safe_limit, "cursor": cursor, "role": role}

        cached_page = await self.cache.get_paginated(**filter_params)
        if cached_page is not None:
            return UserPage(**cached_page)

        cursor_dt: Optional[datetime] = None
        cursor_id: Optional[Any] = None
        if cursor:
            cursor_dt, cursor_id = decode_cursor(cursor)

        users_db, has_next = await self.repo.list_keyset(
            limit=safe_limit,
            cursor_dt=cursor_dt,
            cursor_id=cursor_id,
            role=role,
        )

        items_schema = [self._to_read_schema(u) for u in users_db]

        next_cursor: Optional[str] = None
        if has_next and users_db:
            last_item = users_db[-1]
            if last_item.created_at:
                next_cursor = encode_cursor(last_item.created_at, last_item.id)

        page_result: Dict[str, Any] = {
            "items": [item.model_dump(mode="json") for item in items_schema],
            "next_cursor": next_cursor,
            "limit": safe_limit,
        }

        await self.cache.set_paginated(page_result, **filter_params)
        return UserPage(**page_result)

    # --------------------------------------------------------------------------
    # 4. UPDATE USER (PATCH /users/{user_id})
    # --------------------------------------------------------------------------
    async def update_user(self, user_id: uuid.UUID, payload: UserUpdate) -> UserRead:
        """Partially update user fields with selective re-encryption, cache invalidation, and Outbox sync."""
        user = await self.repo.get(user_id)
        if user is None:
            raise NotFoundError(f"User with ID '{user_id}' not found.")

        updates = payload.model_dump(exclude_unset=True)

        if "email" in updates and updates["email"] is not None:
            existing = await self.repo.get_by_email(str(updates["email"]))
            if existing and str(existing.id) != str(user.id):
                raise ConflictError("A user with this email address already exists.")
            user.email = str(updates["email"])

        if payload.name is not None:
            user.name = payload.name
        if payload.role is not None:
            user.role = payload.role
        if payload.department is not None:
            user.department = payload.department

        if payload.phone is not None:
            if self.crypto is not None:
                aad = f"users.phone:{user.id}".encode("utf-8")
                user.phone_encrypted = self.crypto.encrypt(payload.phone, aad=aad)
                user.phone_blind_index = self.crypto.blind_index(payload.phone)
            else:
                user.phone_encrypted = payload.phone
                user.phone_blind_index = payload.phone

        updated = await self.repo.update(user)
        read_model = self._to_read_schema(updated)

        # Stage Outbox Event BEFORE DB commit
        outbox_event = await self.outbox_repo.create_event(
            aggregate_type="user",
            aggregate_id=str(updated.id),
            event_type="UPDATE",
            payload=self._build_user_es_doc(updated),
        )

        # Invalidate Redis item & list cache
        await self.cache.delete_item(str(user_id))
        await self.cache.delete_item(f"profile:{user_id}")
        await self.cache.invalidate_lists()

        # Atomic commit
        await self.db.commit()

        # Dispatch Outbox sync to Elasticsearch
        await self._dispatch_outbox_sync(outbox_event)

        return read_model

    # --------------------------------------------------------------------------
    # 5. DELETE USER (DELETE /users/{user_id})
    # --------------------------------------------------------------------------
    async def delete_user(self, user_id: uuid.UUID) -> None:
        """Delete user, purge Redis cache, and record DELETE event in Transactional Outbox."""
        outbox_event = await self.outbox_repo.create_event(
            aggregate_type="user",
            aggregate_id=str(user_id),
            event_type="DELETE",
            payload={},
        )

        deleted = await self.repo.delete(user_id)
        if not deleted:
            raise NotFoundError(f"User with ID '{user_id}' not found.")

        # Invalidate Redis
        await self.cache.delete_item(str(user_id))
        await self.cache.delete_item(f"profile:{user_id}")
        await self.cache.invalidate_lists()

        # Atomic commit
        await self.db.commit()

        # Dispatch Outbox sync (Deletes from ES and marks outbox published)
        await self._dispatch_outbox_sync(outbox_event)

    # --------------------------------------------------------------------------
    # 6. SEARCH USERS (GET /users/search - Elasticsearch with PostgreSQL Fallback)
    # --------------------------------------------------------------------------
    async def search_users(
        self,
        q: str,
        limit: int = 20,
        offset: int = 0,
    ) -> List[UserRead]:
        """Search users: Elasticsearch fuzzy search first; if no hits or ES unavailable, fallback to PostgreSQL."""
        cleaned_q = (q or "").strip()
        if not cleaned_q:
            return []

        safe_limit = min(100, max(1, limit))
        safe_offset = max(0, offset)

        # 1. Primary: Elasticsearch Full-Text Fuzzy Search
        try:
            hits, _ = await self.es.search(
                index="user",
                query=cleaned_q,
                limit=safe_limit,
                offset=safe_offset,
            )
            if hits:
                return [
                    UserRead(
                        id=hit["id"],
                        name=hit.get("name", ""),
                        email=hit.get("email", ""),
                        role=hit.get("role", "user"),
                        department=hit.get("department"),
                        department_id=hit.get("department_id") or hit.get("department"),
                        role_id=hit.get("role_id") or hit.get("role"),
                        phone_masked=hit.get("phone_masked"),
                        created_at=(
                            datetime.fromisoformat(hit["created_at"])
                            if isinstance(hit.get("created_at"), str)
                            else hit.get("created_at")
                        ),
                    )
                    for hit in hits
                ]
        except Exception as exc:
            logger.warning("Elasticsearch search failed for '%s': %s. Falling back to PostgreSQL.", cleaned_q, exc)

        # 2. Secondary Fallback: PostgreSQL Direct Database Search
        users = await self.repo.search(
            query=cleaned_q,
            limit=safe_limit,
            offset=safe_offset,
        )
        return [self._to_read_schema(u) for u in users]

    

    # --------------------------------------------------------------------------
    # Helper Mappings & CQRS Synchronization
    # --------------------------------------------------------------------------
    def _to_read_schema(self, user: User) -> UserRead:
        """Decrypts phone using AAD and returns safe masked representation."""
        masked_phone = user.phone_encrypted
        if self.crypto is not None and user.phone_encrypted:
            aad = f"users.phone:{user.id}".encode("utf-8")
            try:
                plain_phone = self.crypto.decrypt(user.phone_encrypted, aad=aad)
                masked_phone = self.crypto.mask_phone(plain_phone)
            except DecryptionError:
                masked_phone = "***"

        return UserRead(
            id=user.id,
            name=user.name,
            email=user.email,
            role=user.role,
            department=user.department,
            department_id=user.department_id or user.department,
            role_id=user.role_id or user.role,
            phone_masked=masked_phone,
            created_at=user.created_at or datetime.now(timezone.utc),
        )

    def _build_user_es_doc(self, user: User) -> Dict[str, Any]:
        """Constructs Elasticsearch document shape for User indexing."""
        return {
            "id": str(user.id),
            "name": user.name,
            "email": user.email,
            "role": user.role,
            "department": user.department,
            "created_at": user.created_at.isoformat() if user.created_at else None,
        }

    async def _dispatch_outbox_sync(self, event: OutboxEvent) -> None:
        """Dispatches outbox event synchronization to Elasticsearch."""
        if self.outbox_worker is not None:
            asyncio.create_task(self.outbox_worker.sync_now(event.id))
        else:
            try:
                agg_id = str(event.aggregate_id)
                if event.event_type in ("CREATE", "UPDATE"):
                    if event.payload:
                        await self.es.index_document(index="user", id_=agg_id, document=event.payload)
                elif event.event_type == "DELETE":
                    await self.es.delete_document(index="user", id_=agg_id)

                now = datetime.now(timezone.utc)
                await self.outbox_repo.mark_published(event.id, published_at=now)
            except Exception as exc:
                logger.warning("Direct outbox sync fallback failed: %s", exc)


# ------------------------------------------------------------------------------
# User Domain Dependencies (FastAPI Official Documentation Style)
# ------------------------------------------------------------------------------
def get_user_cache(
    redis: RedisDep,
    settings: Annotated[Settings, Depends(get_settings)],
) -> ICacheService:
    """Dependency constructing a DynamicRedisCache for the user domain."""
    if redis is not None:
        return DynamicRedisCache(
            redis=redis,
            prefix="users",
            item_ttl=settings.CACHE_ITEM_TTL,
            list_ttl=settings.CACHE_LIST_TTL,
            negative_ttl=settings.CACHE_NEGATIVE_TTL,
            lock_ttl_ms=settings.CACHE_LOCK_TTL_MS,
            flags=settings.get_cache_flags(),
        )
    return NullCache()


UserCacheDep = Annotated[ICacheService, Depends(get_user_cache)]


def get_user_service(
    session: SessionDep,
    cache: UserCacheDep,
    crypto: CryptoDep,
    es: ElasticsearchDep,
    request: Request,
) -> UserService:
    """Dependency composing database, Redis, AES-256-GCM, Outbox, and Elasticsearch into UserService."""
    worker = None
    if hasattr(request, "app") and hasattr(request.app, "state"):
        worker = getattr(request.app.state, "outbox_worker", None)

    return UserService(
        db=session,
        cache=cache,
        crypto=crypto,
        elasticsearch=es,
        outbox_worker=worker,
    )


UserServiceDep = Annotated[UserService, Depends(get_user_service)]


__all__ = [
    "UserService",
    "get_user_cache",
    "UserCacheDep",
    "get_user_service",
    "UserServiceDep",
]
