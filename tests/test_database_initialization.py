"""Tests for safe schema checks, opt-in table creation, and async URL support."""

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from app.core.config import Settings
from app.database.initialization import (
    check_database_connection,
    create_missing_tables,
    get_missing_tables,
    initialize_database,
)
from app.db.session import create_session_factory, normalize_async_database_url


@pytest.mark.asyncio
async def test_create_tables_only_creates_missing_metadata_tables() -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    try:
        async with engine.begin() as connection:
            await connection.execute(
                text("CREATE TABLE categories (id INTEGER PRIMARY KEY)")
            )
            await connection.execute(
                text("INSERT INTO categories (id) VALUES (17)")
            )

        missing_before = await get_missing_tables(engine)
        assert "categories" not in missing_before
        assert "products" in missing_before

        created = await create_missing_tables(engine)
        assert set(created) == set(missing_before)
        assert await get_missing_tables(engine) == []
        async with engine.connect() as connection:
            preserved_id = await connection.scalar(
                text("SELECT id FROM categories WHERE id = 17")
            )
        assert preserved_id == 17
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_create_tables_refuses_to_bypass_alembic_history() -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    try:
        async with engine.begin() as connection:
            await connection.execute(
                text("CREATE TABLE alembic_version (version_num VARCHAR(32) PRIMARY KEY)")
            )

        with pytest.raises(RuntimeError, match="Alembic-managed"):
            await create_missing_tables(engine)
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_initialize_checks_connectivity_and_warns_without_implicit_creation(
    caplog: pytest.LogCaptureFixture,
) -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    settings = Settings(
        DATABASE_URL="sqlite+aiosqlite:///:memory:",
        AUTO_MIGRATE=False,
        AUTO_CREATE_TABLES=False,
        AUTO_SEED=False,
    )
    try:
        await check_database_connection(engine)
        await initialize_database(engine, create_session_factory(engine), settings)
        assert "Database tables are missing" in caplog.text
        assert await get_missing_tables(engine)
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_initialize_creates_tables_only_when_enabled() -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    settings = Settings(
        DATABASE_URL="sqlite+aiosqlite:///:memory:",
        AUTO_MIGRATE=False,
        AUTO_CREATE_TABLES=True,
        AUTO_SEED=False,
    )
    try:
        await initialize_database(engine, create_session_factory(engine), settings)
        assert await get_missing_tables(engine) == []
    finally:
        await engine.dispose()


def test_auto_initialization_flags_default_to_false() -> None:
    settings = Settings(DATABASE_URL="sqlite+aiosqlite:///:memory:")
    assert settings.AUTO_MIGRATE is False
    assert settings.AUTO_CREATE_TABLES is False
    assert settings.AUTO_SEED is False


@pytest.mark.parametrize(
    ("source", "expected"),
    [
        ("postgresql://user:pass@localhost/app", "postgresql+asyncpg"),
        ("mysql+pymysql://user:pass@localhost/app", "mysql+asyncmy"),
        ("mysql://user:pass@localhost/app", "mysql+asyncmy"),
        ("sqlite:///:memory:", "sqlite+aiosqlite"),
    ],
)
def test_database_url_uses_supported_async_drivers(source: str, expected: str) -> None:
    normalized = normalize_async_database_url(source)
    assert normalized.startswith(expected + "://")
