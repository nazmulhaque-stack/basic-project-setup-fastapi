"""Optional, non-overwriting user reference-data seeder."""

from typing import ClassVar
import uuid

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import get_settings
from app.core.security import CryptoService
from app.database.seeders.base import BaseSeeder
from app.database.seeders.registry import register_seeder
from app.models.user import User


@register_seeder
class UserSeeder(BaseSeeder):
    """Seed initial administrator and service accounts without changing existing rows."""

    name: ClassVar[str] = "user_seeder"
    dependencies: ClassVar[tuple[str, ...]] = ("category_seeder",)

    DEFAULT_USERS: ClassVar[tuple[dict[str, str], ...]] = (
        {
            "name": "System Administrator",
            "email": "admin@example.com",
            "phone": "+15550001111",
            "role": "admin",
        },
        {
            "name": "Integration Service",
            "email": "service@example.com",
            "phone": "+15550002222",
            "role": "service",
        },
    )

    async def seed(self, session: AsyncSession) -> None:
        settings = get_settings()
        crypto = CryptoService(
            keys=settings.get_encryption_keys(),
            active_version=settings.ACTIVE_KEY_VERSION,
            blind_index_key=settings.get_blind_index_key(),
        )

        for user_data in self.DEFAULT_USERS:
            # Check by email to prevent duplicate inserts and preserve existing records
            existing = await session.scalar(
                select(User.id).where(User.email == user_data["email"])
            )
            if existing is None:
                user_id = uuid.uuid4()
                aad = f"users.phone:{user_id}".encode("utf-8")
                encrypted_phone = crypto.encrypt(user_data["phone"], aad=aad)
                blind_index = crypto.blind_index(user_data["phone"])

                user = User(
                    id=user_id,
                    name=user_data["name"],
                    email=user_data["email"],
                    role=user_data["role"],
                    phone_encrypted=encrypted_phone,
                    phone_blind_index=blind_index,
                )
                session.add(user)

        await session.flush()


__all__ = ["UserSeeder"]
