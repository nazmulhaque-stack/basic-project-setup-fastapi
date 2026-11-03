"""User Service implementing domain business rules, PII cryptography, and validation.

Leverages the Template Method Pattern provided by BaseService to eliminate duplicate CRUD boilerplate.
"""

from datetime import datetime, timezone
import logging
from typing import Any, Dict, Optional
import uuid

from app.core.elasticsearch import IElasticsearchService
from app.core.exceptions import ConflictError
from app.core.security import CryptoService, DecryptionError
from app.db.pool import UnitOfWork
from app.models.user import User
from app.repositories.user import UserRepository
from app.schemas.user import UserCreate, UserPage, UserRead, UserUpdate
from app.services.base import BaseService
from app.services.interface import ICacheService

logger = logging.getLogger(__name__)


class UserService(BaseService[User, UserCreate, UserUpdate, UserRead, uuid.UUID]):
    """User domain service orchestrating repository CRUD, PII encryption, and caching."""

    def __init__(
        self,
        db: Optional[UnitOfWork] = None,
        repo: Optional[UserRepository] = None,
        cache: Optional[ICacheService] = None,
        crypto: Optional[CryptoService] = None,
        elasticsearch: Optional[IElasticsearchService] = None,
        outbox_enabled: bool = False,
        auto_outbox_events: bool = False,
        outbox_topic: str = "domain.events",
        service_name: Optional[str] = None,
    ) -> None:
        resolved_repo: UserRepository
        if repo is not None:
            resolved_repo = repo
        elif db is not None:
            resolved_repo = UserRepository(db)
        else:
            raise ValueError("Either db session or repo must be provided to UserService")

        super().__init__(
            repo=resolved_repo,
            read_schema_cls=UserRead,
            cache=cache,
            elasticsearch=elasticsearch,
            order_col="created_at",
            id_col="id",
            db=db,
            outbox_enabled=outbox_enabled,
            auto_outbox_events=auto_outbox_events,
            outbox_topic=outbox_topic,
            service_name=service_name,
        )
        self.repository: UserRepository = resolved_repo
        self.crypto: Optional[CryptoService] = crypto

    repository_class = UserRepository


    # --------------------------------------------------------------------------
    # Pluggable Domain Lifecycle Hooks (Template Method Pattern)
    # --------------------------------------------------------------------------

    async def before_create(self, payload: UserCreate) -> User:
        """Domain validation, client UUID generation, AES-256-GCM encryption, and HMAC blind indexing."""
        existing = await self.repository.get_by_email(str(payload.email))
        if existing:
            raise ConflictError("A user with this email address already exists.")

        user_id: uuid.UUID = uuid.uuid4()
        phone_encrypted: str
        phone_blind_index: str

        if self.crypto is not None:
            blind_index: str = self.crypto.blind_index(payload.phone)
            existing_phone = await self.repository.get_by_phone_blind_index(blind_index)
            if existing_phone:
                raise ConflictError("A user with this phone number already exists.")

            aad: bytes = f"users.phone:{user_id}".encode("utf-8")
            phone_encrypted = self.crypto.encrypt(payload.phone, aad=aad)
            phone_blind_index = blind_index
        else:
            phone_encrypted = payload.phone
            phone_blind_index = payload.phone

        return User(
            id=user_id,
            name=payload.name,
            email=payload.email,
            role=payload.role,
            phone_encrypted=phone_encrypted,
            phone_blind_index=phone_blind_index,
            created_at=datetime.now(timezone.utc),
        )

    async def before_update(self, entity: User, payload: UserUpdate) -> None:
        """Validates email uniqueness and updates user fields with selective re-encryption."""
        updates = payload.model_dump(exclude_unset=True)
        if "email" in updates and updates["email"] is not None:
            existing = await self.repository.get_by_email(str(updates["email"]))
            if existing and str(existing.id) != str(entity.id):
                raise ConflictError("Email already exists")

        if payload.name is not None:
            entity.name = payload.name
        if getattr(payload, "email", None) is not None:
            entity.email = str(payload.email)
        if payload.role is not None:
            entity.role = payload.role
        if payload.phone is not None:
            if self.crypto is not None:
                aad: bytes = f"users.phone:{entity.id}".encode("utf-8")
                entity.phone_encrypted = self.crypto.encrypt(payload.phone, aad=aad)
                entity.phone_blind_index = self.crypto.blind_index(payload.phone)
            else:
                entity.phone_encrypted = payload.phone
                entity.phone_blind_index = payload.phone

    async def to_read_schema(self, entity: User) -> UserRead:
        """Decrypts phone using AAD and masks PII for public output."""
        masked_phone: str
        if self.crypto is not None:
            aad: bytes = f"users.phone:{entity.id}".encode("utf-8")
            try:
                plain_phone: str = self.crypto.decrypt(entity.phone_encrypted, aad=aad)
                masked_phone = self.crypto.mask_phone(plain_phone)
            except DecryptionError:
                masked_phone = "***"
        else:
            masked_phone = entity.phone_encrypted

        return UserRead(
            id=entity.id,
            name=entity.name,
            email=entity.email,
            role=entity.role,
            phone_masked=masked_phone,
            created_at=entity.created_at or datetime.now(timezone.utc),
        )

    async def to_cache_dict(self, entity: User) -> Dict[str, Any]:
        """Serializes safe masked user read schema for Redis item caching."""
        read_model: UserRead = await self.to_read_schema(entity)
        return read_model.model_dump(mode="json")

    # --------------------------------------------------------------------------
    # Keyset Pagination Query
    # --------------------------------------------------------------------------

    async def list_users(
        self,
        limit: int = 50,
        cursor: Optional[str] = None,
        role: Optional[str] = None,
        cache_enabled: Optional[bool] = None,
    ) -> UserPage:
        """Keyset cursor-paginated user list with cache-aside support."""
        filters: Dict[str, Any] = {}
        if role:
            filters["role"] = role

        page_dict: Dict[str, Any] = await self.list_keyset(
            limit=limit,
            cursor=cursor,
            cache_enabled=cache_enabled,
            **filters,
        )
        return UserPage(**page_dict)

    # --------------------------------------------------------------------------
    # Backward-Compatible Method Aliases Directly Bound from BaseService
    # --------------------------------------------------------------------------
    create_user = BaseService.create
    get_user_by_id = BaseService.get_by_id
    update_user = BaseService.update_by_id
    delete_user = BaseService.delete_by_id


__all__ = ["UserService"]
