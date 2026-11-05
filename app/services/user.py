"""User Service: Clean domain orchestration with direct Redis caching and Elasticsearch search (no OOP BaseService)."""

from datetime import datetime, timezone
import logging
from typing import Annotated, Any, Dict, List, Optional
import uuid

from fastapi import Depends

from app.api.deps import CryptoDep, ElasticsearchDep, RedisDep, SessionDep
from app.core.config import Settings, get_settings
from app.core.elasticsearch import IElasticsearchService, NullElasticsearch
from app.core.exceptions import ConflictError, DecryptionError, NotFoundError
from app.core.security import CryptoService
from app.db.pool import UnitOfWork
from app.models.user import User
from app.repositories.user import UserRepository
from app.schemas.base import decode_cursor, encode_cursor
from app.schemas.user import UserCreate, UserPage, UserRead, UserUpdate
from app.core.cache_interface import ICacheService, NullCache
from app.services.cache import DynamicRedisCache

logger = logging.getLogger(__name__)


class UserService:
    """User business logic, PII encryption, Redis caching, and Elasticsearch search."""

    def __init__(
        self,
        db: UnitOfWork,
        cache: Optional[ICacheService] = None,
        crypto: Optional[CryptoService] = None,
        elasticsearch: Optional[IElasticsearchService] = None,
        **_: Any,
    ) -> None:
        self.db = db
        self.repo = UserRepository(db)
        self.cache: ICacheService = cache or NullCache()
        self.crypto: Optional[CryptoService] = crypto
        self.es: IElasticsearchService = elasticsearch or NullElasticsearch()

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
            phone_masked=masked_phone,
            created_at=user.created_at or datetime.now(timezone.utc),
        )

    async def create_user(self, payload: UserCreate) -> UserRead:
        """Create a user with PII encryption, blind index, direct ES indexing, and Redis invalidation."""
        # 1. Uniqueness check for email
        existing_email = await self.repo.get_by_email(str(payload.email))
        if existing_email:
            raise ConflictError("A user with this email address already exists.")

        user_id = uuid.uuid4()
        phone_encrypted = payload.phone
        phone_blind_index = payload.phone

        # 2. Cryptographic phone encryption & HMAC blind index
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
            phone_encrypted=phone_encrypted,
            phone_blind_index=phone_blind_index,
            created_at=datetime.now(timezone.utc),
        )

        # 3. Insert into PostgreSQL
        created = await self.repo.create(user)
        read_model = self._to_read_schema(created)

        # 4. Direct indexing into Elasticsearch (CQRS)
        await self.es.index_document(
            index="user",
            id_=str(created.id),
            document={
                "id": str(created.id),
                "name": created.name,
                "email": created.email,
                "role": created.role,
                "created_at": created.created_at.isoformat() if created.created_at else None,
            },
        )

        # 5. Invalidate Redis list cache and store item
        await self.cache.invalidate_lists()
        await self.cache.set_item(str(created.id), read_model.model_dump(mode="json"))

        # 6. Commit Database Transaction
        await self.db.commit()

        return read_model

    async def get_user_by_id(self, user_id: uuid.UUID) -> UserRead:
        """Fetch user by ID with Redis cache-aside (sub-millisecond read)."""
        cached = await self.cache.get_item(str(user_id))
        if cached is not None:
            return UserRead(**cached)

        user = await self.repo.get(user_id)
        if user is None:
            await self.cache.set_negative_cache(str(user_id))
            raise NotFoundError(f"User with ID '{user_id}' not found.")

        read_model = self._to_read_schema(user)
        await self.cache.set_item(str(user.id), read_model.model_dump(mode="json"))
        return read_model

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

    async def search_users(
        self,
        q: str,
        limit: int = 20,
        offset: int = 0,
    ) -> List[UserRead]:
        """Full-text fuzzy search offloaded directly to Elasticsearch."""
        hits, total = await self.es.search(
            index="user",
            query=q,
            limit=limit,
            offset=offset,
        )
        results: List[UserRead] = []
        for hit in hits:
            results.append(
                UserRead(
                    id=hit["id"],
                    name=hit.get("name", ""),
                    email=hit.get("email", ""),
                    role=hit.get("role", "user"),
                    phone_masked=None,
                    created_at=datetime.fromisoformat(hit["created_at"]) if hit.get("created_at") else None,
                )
            )
        return results

    async def update_user(self, user_id: uuid.UUID, payload: UserUpdate) -> UserRead:
        """Partially update user fields with selective re-encryption and cache invalidation."""
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

        # Direct ES document update
        await self.es.index_document(
            index="user",
            id_=str(updated.id),
            document={
                "id": str(updated.id),
                "name": updated.name,
                "email": updated.email,
                "role": updated.role,
                "created_at": updated.created_at.isoformat() if updated.created_at else None,
            },
        )

        # Invalidate Redis item & list cache
        await self.cache.delete_item(str(user_id))
        await self.cache.invalidate_lists()

        # Commit DB transaction
        await self.db.commit()

        return read_model

    async def delete_user(self, user_id: uuid.UUID) -> None:
        """Delete user, purge Redis cache, and delete document from Elasticsearch."""
        deleted = await self.repo.delete(user_id)
        if not deleted:
            raise NotFoundError(f"User with ID '{user_id}' not found.")

        # Delete from Elasticsearch
        await self.es.delete_document(index="user", id_=str(user_id))

        # Invalidate Redis
        await self.cache.delete_item(str(user_id))
        await self.cache.invalidate_lists()

        # Commit DB transaction
        await self.db.commit()


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
) -> UserService:
    """Dependency composing database, Redis, AES-256-GCM, and Elasticsearch into UserService."""
    return UserService(
        db=session,
        cache=cache,
        crypto=crypto,
        elasticsearch=es,
    )


UserServiceDep = Annotated[UserService, Depends(get_user_service)]


__all__ = [
    "UserService",
    "get_user_cache",
    "UserCacheDep",
    "get_user_service",
    "UserServiceDep",
]
