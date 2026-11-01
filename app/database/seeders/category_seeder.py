"""Optional, non-overwriting category reference-data seeder."""

import json
from typing import ClassVar

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import get_settings
from app.database.seeders.base import BaseSeeder
from app.database.seeders.registry import register_seeder
from app.models.category import Category


@register_seeder
class CategorySeeder(BaseSeeder):
    """Seed configured category names without changing existing category rows."""

    name: ClassVar[str] = "category_seeder"

    async def seed(self, session: AsyncSession) -> None:
        try:
            configured_names = json.loads(get_settings().SEED_CATEGORIES)
        except json.JSONDecodeError as exc:
            raise ValueError("SEED_CATEGORIES must be a JSON array of strings.") from exc
        if not isinstance(configured_names, list) or any(
            not isinstance(name, str) for name in configured_names
        ):
            raise ValueError("SEED_CATEGORIES must be a JSON array of strings.")

        seen: set[str] = set()
        for raw_name in configured_names:
            name = raw_name.strip()
            if not name or name in seen:
                continue
            seen.add(name)
            existing = await session.scalar(
                select(Category.id).where(Category.name == name)
            )
            if existing is None:
                session.add(Category(name=name, is_active=True))
        await session.flush()


__all__ = ["CategorySeeder"]
