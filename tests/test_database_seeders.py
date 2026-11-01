"""Tests for dependency-ordered, transactional, idempotent seeders."""

from types import SimpleNamespace

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.database.seeders.base import BaseSeeder
from app.database.seeders.registry import order_seeders
from app.database.seeders.runner import run_seeders
from app.db.base import Base
from app.models.category import Category


class FirstSeeder(BaseSeeder):
    name = "first"

    async def seed(self, session) -> None:
        pass


class DependentSeeder(BaseSeeder):
    name = "dependent"
    dependencies = ("first",)

    async def seed(self, session) -> None:
        pass


def test_seeders_are_ordered_by_dependencies() -> None:
    ordered = order_seeders(
        {"dependent": DependentSeeder, "first": FirstSeeder},
        requested=["dependent"],
    )
    assert [seeder.name for seeder in ordered] == ["first", "dependent"]


def test_seeders_reject_cycles_and_missing_dependencies() -> None:
    class CircularSeeder(BaseSeeder):
        name = "circular"
        dependencies = ("missing",)

        async def seed(self, session) -> None:
            pass

    with pytest.raises(ValueError, match="Unknown seeder"):
        order_seeders({"circular": CircularSeeder})

    class CycleA(BaseSeeder):
        name = "a"
        dependencies = ("b",)

        async def seed(self, session) -> None:
            pass

    class CycleB(BaseSeeder):
        name = "b"
        dependencies = ("a",)

        async def seed(self, session) -> None:
            pass

    with pytest.raises(ValueError, match="Circular"):
        order_seeders({"a": CycleA, "b": CycleB})


@pytest.mark.asyncio
async def test_category_seeder_is_idempotent_and_does_not_overwrite(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from app.database.seeders import category_seeder

    monkeypatch.setattr(
        category_seeder,
        "get_settings",
        lambda: SimpleNamespace(SEED_CATEGORIES='["Electronics", " Books ", "Books"]'),
    )
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)

    async with session_factory() as session:
        session.add(Category(name="Electronics", description="User-owned description"))
        await session.commit()

    assert await run_seeders(session_factory, ["category_seeder"]) == [
        "category_seeder"
    ]
    assert await run_seeders(session_factory, ["category_seeder"]) == [
        "category_seeder"
    ]

    async with session_factory() as session:
        categories = (
            await session.scalars(select(Category).order_by(Category.name))
        ).all()
        assert [(category.name, category.description) for category in categories] == [
            ("Books", None),
            ("Electronics", "User-owned description"),
        ]
    await engine.dispose()
