"""User Service: Clean domain orchestration with direct Redis caching and Elasticsearch search (no OOP BaseService)."""

from datetime import datetime, timezone
import logging
from typing import Annotated, Any, Dict, List, Optional
import uuid

from fastapi import Depends

from app.api.deps import CryptoDep, ElasticsearchDep, RedisDep, SessionDep
from app.core.cache_interface import ICacheService, NullCache
from app.core.config import Settings, get_settings
from app.core.elasticsearch import IElasticsearchService, NullElasticsearch
from app.core.exceptions import ConflictError, DecryptionError, NotFoundError
from app.core.security import CryptoService
from app.db.pool import UnitOfWork
from app.models.user import Department, Role, User, UserAccount, UserProfile
from app.repositories.user import UserRepository
from app.schemas.base import decode_cursor, encode_cursor
from app.schemas.user import (
    AccountCreate,
    AccountRead,
    DepartmentRead,
    RoleRead,
    UserCreate,
    UserPage,
    UserProfileRead,
    UserRead,
    UserUpdate,
)
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
            department=user.department,
            department_id=user.department_id or user.department,
            role_id=user.role_id or user.role,
            phone_masked=masked_phone,
            created_at=user.created_at or datetime.now(timezone.utc),
        )

    def _to_profile_read(self, profile: UserProfile) -> UserProfileRead:
        """Transforms rich aggregated UserProfile domain dataclass into UserProfileRead schema."""
        user = profile.user
        masked_phone = user.phone_encrypted
        if self.crypto is not None and user.phone_encrypted:
            aad = f"users.phone:{user.id}".encode("utf-8")
            try:
                plain_phone = self.crypto.decrypt(user.phone_encrypted, aad=aad)
                masked_phone = self.crypto.mask_phone(plain_phone)
            except DecryptionError:
                masked_phone = "***"

        dept_read: Optional[DepartmentRead] = None
        if profile.department:
            dept_read = DepartmentRead(
                id=profile.department.id,
                name=profile.department.name,
                code=profile.department.code,
                description=profile.department.description,
            )

        role_read: Optional[RoleRead] = None
        if profile.role_info:
            role_read = RoleRead(
                id=profile.role_info.id,
                name=profile.role_info.name,
                description=profile.role_info.description,
                permissions=profile.role_info.permissions,
            )

        accounts_read: List[AccountRead] = [
            AccountRead(
                id=acc.id,
                account_number=acc.account_number,
                account_type=acc.account_type,
                balance_cents=acc.balance_cents,
                currency=acc.currency,
                status=acc.status,
                created_at=acc.created_at,
            )
            for acc in profile.accounts
        ]

        return UserProfileRead(
            id=user.id,
            name=user.name,
            email=user.email,
            role=user.role,
            phone_masked=masked_phone,
            department=dept_read,
            role_info=role_read,
            accounts=accounts_read,
            total_balance_cents=profile.total_balance_cents,
            account_count=len(profile.accounts),
            created_at=user.created_at or datetime.now(timezone.utc),
        )

    # --------------------------------------------------------------------------
    # Elasticsearch Synchronization (Write Path)
    # --------------------------------------------------------------------------
    async def _sync_user_to_es(self, user_id: uuid.UUID) -> None:
        """Synchronizes user entity and aggregated profile document to Elasticsearch."""
        profile = await self.repo.get_profile(user_id)
        if profile is None:
            return

        user = profile.user
        # 1. Sync 'user' index
        await self.es.index_document(
            index="user",
            id_=str(user.id),
            document={
                "id": str(user.id),
                "name": user.name,
                "email": user.email,
                "role": user.role,
                "department": user.department,
                "created_at": user.created_at.isoformat() if user.created_at else None,
            },
        )

        # 2. Sync 'user_profile' index (Full denormalized CQRS document)
        profile_read = self._to_profile_read(profile)
        doc = profile_read.model_dump(mode="json")
        doc["department_name"] = profile_read.department.name if profile_read.department else None
        doc["department_code"] = profile_read.department.code if profile_read.department else None
        doc["role_name"] = profile_read.role_info.name if profile_read.role_info else None
        doc["account_numbers"] = [a.account_number for a in profile_read.accounts]
        await self.es.index_document(
            index="user_profile",
            id_=str(user.id),
            document=doc,
        )

    async def sync_all_to_es(self) -> Dict[str, Any]:
        """Bulk synchronizes all users and profiles from PostgreSQL into Elasticsearch."""
        profiles = await self.repo.list_all_profiles()
        count = 0
        for profile in profiles:
            await self._sync_user_to_es(profile.user.id)
            count += 1
        return {"status": "success", "synced_count": count}

    # --------------------------------------------------------------------------
    # User CRUD
    # --------------------------------------------------------------------------
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
            department=payload.department,
            department_id=None,
            role_id=None,
            phone_encrypted=phone_encrypted,
            phone_blind_index=phone_blind_index,
            created_at=datetime.now(timezone.utc),
        )

        # 3. Insert into PostgreSQL
        created = await self.repo.create(user)

        # 4. Insert optional initial accounts
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

        # 5. Invalidate Redis list cache and store item
        await self.cache.invalidate_lists()
        await self.cache.set_item(str(created.id), read_model.model_dump(mode="json"))

        # 6. Commit Database Transaction
        await self.db.commit()

        # 7. Sync Elasticsearch (both user and user_profile documents)
        await self._sync_user_to_es(created.id)

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

    # --------------------------------------------------------------------------
    # Aggregated Profile with Redis Cache-Aside & Elasticsearch CQRS
    # --------------------------------------------------------------------------
    async def get_user_profile(self, user_id: uuid.UUID) -> UserProfileRead:
        """CQRS Profile Read: Queries Redis Cache-Aside first, then directly retrieves from Elasticsearch (NOT PostgreSQL)."""
        cache_key = f"profile:{user_id}"

        # 1. Level 1: Redis Cache-Aside check
        cached = await self.cache.get_item(cache_key)
        if cached is not None:
            return UserProfileRead(**cached)

        # 2. Level 2: CQRS Read directly from Elasticsearch (Zero PostgreSQL queries)
        es_doc = await self.es.get_document(index="user_profile", id_=str(user_id))
        if es_doc is not None:
            profile_read = UserProfileRead(**es_doc)
            # Populate L1 Redis Cache
            await self.cache.set_item(cache_key, profile_read.model_dump(mode="json"))
            return profile_read

        # 3. Fallback only if ES document is absent:
        profile = await self.repo.get_profile(user_id)
        if profile is None:
            await self.cache.set_negative_cache(cache_key)
            raise NotFoundError(f"User with ID '{user_id}' not found.")

        profile_read = self._to_profile_read(profile)
        await self.cache.set_item(cache_key, profile_read.model_dump(mode="json"))
        await self._sync_user_to_es(user_id)
        return profile_read

    async def search_user_profiles(
        self,
        q: str,
        limit: int = 20,
        offset: int = 0,
    ) -> List[UserProfileRead]:
        """Full-text fuzzy search across aggregated profiles directly in Elasticsearch."""
        hits, total = await self.es.search(
            index="user_profile",
            query=q,
            limit=limit,
            offset=offset,
        )
        results: List[UserProfileRead] = []
        for hit in hits:
            try:
                results.append(UserProfileRead(**hit))
            except Exception:
                pass
        return results

    async def add_user_account(self, user_id: uuid.UUID, payload: AccountCreate) -> AccountRead:
        """Adds account info to user, invalidates profile cache, updates ES, and commits transaction."""
        user = await self.repo.get(user_id)
        if user is None:
            raise NotFoundError(f"User with ID '{user_id}' not found.")

        account = UserAccount(
            id=uuid.uuid4(),
            user_id=user_id,
            account_number=payload.account_number,
            account_type=payload.account_type,
            balance_cents=payload.balance_cents,
            currency=payload.currency,
            status=payload.status,
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
        )
        created = await self.repo.create_account(account)

        # Invalidate profile cache so next read fetches updated accounts
        await self.cache.delete_item(f"profile:{user_id}")

        # Commit DB transaction
        await self.db.commit()

        # Sync Elasticsearch profile document immediately on write
        await self._sync_user_to_es(user_id)

        return AccountRead(
            id=created.id,
            account_number=created.account_number,
            account_type=created.account_type,
            balance_cents=created.balance_cents,
            currency=created.currency,
            status=created.status,
            created_at=created.created_at,
        )

    async def list_departments(self) -> List[DepartmentRead]:
        """List all departments."""
        depts = await self.repo.list_departments()
        return [
            DepartmentRead(
                id=d.id,
                name=d.name,
                code=d.code,
                description=d.description,
            )
            for d in depts
        ]

    async def list_roles(self) -> List[RoleRead]:
        """List all roles and permission sets."""
        roles = await self.repo.list_roles()
        return [
            RoleRead(
                id=r.id,
                name=r.name,
                description=r.description,
                permissions=r.permissions,
            )
            for r in roles
        ]

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
                    department=hit.get("department"),
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

        # Invalidate Redis item, profile, & list cache
        await self.cache.delete_item(str(user_id))
        await self.cache.delete_item(f"profile:{user_id}")
        await self.cache.invalidate_lists()

        # Commit DB transaction
        await self.db.commit()

        # Sync Elasticsearch (both user and user_profile documents) immediately on write
        await self._sync_user_to_es(updated.id)

        return read_model

    async def delete_user(self, user_id: uuid.UUID) -> None:
        """Delete user, purge Redis cache, and delete document from Elasticsearch."""
        deleted = await self.repo.delete(user_id)
        if not deleted:
            raise NotFoundError(f"User with ID '{user_id}' not found.")

        # Invalidate Redis
        await self.cache.delete_item(str(user_id))
        await self.cache.delete_item(f"profile:{user_id}")
        await self.cache.invalidate_lists()

        # Commit DB transaction
        await self.db.commit()

        # Delete from Elasticsearch immediately on write
        await self.es.delete_document(index="user", id_=str(user_id))
        await self.es.delete_document(index="user_profile", id_=str(user_id))


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
