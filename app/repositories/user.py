"""User Repository implementing domain-specific queries via BaseRepository."""

from typing import Optional
import uuid
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.user import User
from app.repositories.base import BaseRepository


class UserRepository(BaseRepository[User, uuid.UUID]):
    """Concrete repository for User domain entity extending BaseRepository."""

    def __init__(self, db: AsyncSession) -> None:
        super().__init__(model=User, db=db)

    async def get_by_email(self, email: str) -> Optional[User]:
        """Finds user by unique email address."""
        stmt = select(User).where(User.email == email)
        return await self.db.scalar(stmt)

    async def get_by_phone_blind_index(self, blind_index: str) -> Optional[User]:
        """Finds user by deterministic blind index of encrypted phone."""
        stmt = select(User).where(User.phone_blind_index == blind_index)
        return await self.db.scalar(stmt)


__all__ = ["UserRepository"]
