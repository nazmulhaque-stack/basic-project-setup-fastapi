"""Base interface for transaction-scoped, idempotent database seeders."""

from abc import ABC, abstractmethod
from typing import ClassVar

from sqlalchemy.ext.asyncio import AsyncSession


class BaseSeeder(ABC):
    """Seeder implementations must insert only missing records and never commit."""

    name: ClassVar[str]
    dependencies: ClassVar[tuple[str, ...]] = ()

    @abstractmethod
    async def seed(self, session: AsyncSession) -> None:
        """Insert missing seed data using the caller's transaction."""


__all__ = ["BaseSeeder"]
