"""Safe database connectivity, schema inspection, and opt-in initialization."""

import asyncio
import logging

from sqlalchemy import inspect, select, text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from app.core.config import Settings
from app.db.base import Base
from app.models import (  # noqa: F401
    Category,
    InventoryReservation,
    OutboxEvent,
    Product,
    User,
)

logger = logging.getLogger(__name__)


async def check_database_connection(engine: AsyncEngine) -> None:
    """Fail explicitly when the database cannot execute a trivial query."""
    async with engine.connect() as connection:
        await connection.execute(select(text("1")))


async def get_missing_tables(engine: AsyncEngine) -> list[str]:
    """Return registered application tables absent from the connected database."""
    async with engine.connect() as connection:
        existing = await connection.run_sync(
            lambda sync_connection: set(inspect(sync_connection).get_table_names())
        )
    return sorted(set(Base.metadata.tables) - existing)


async def create_missing_tables(engine: AsyncEngine) -> list[str]:
    """Create missing metadata tables without altering or dropping existing tables.

    An Alembic-managed database must be upgraded through migrations instead; using
    create_all against it could create an incomplete schema outside migration history.
    """
    dialect = engine.dialect.name
    async with engine.begin() as connection:
        mysql_lock_acquired = False
        if dialect == "postgresql":
            await connection.execute(
                text("SELECT pg_advisory_xact_lock(:key)"),
                {"key": 738291047},
            )
        elif dialect == "mysql":
            acquired = await connection.scalar(
                text("SELECT GET_LOCK('fastapi_schema_creation', 60)")
            )
            if acquired != 1:
                raise RuntimeError("Could not acquire the database schema-creation lock.")
            mysql_lock_acquired = True

        try:
            existing = await connection.run_sync(
                lambda sync_connection: set(inspect(sync_connection).get_table_names())
            )
            missing = sorted(set(Base.metadata.tables) - existing)
            if not missing:
                return []

            if "alembic_version" in existing:
                raise RuntimeError(
                    "Missing tables were found in an Alembic-managed database. "
                    "Run `python -m app.database migrate` instead of create-tables."
                )

            tables = [Base.metadata.tables[name] for name in missing]
            await connection.run_sync(
                lambda sync_connection: Base.metadata.create_all(
                    sync_connection,
                    tables=tables,
                    checkfirst=True,
                )
            )
        finally:
            if mysql_lock_acquired:
                await connection.execute(
                    text("SELECT RELEASE_LOCK('fastapi_schema_creation')")
                )
    return missing


async def initialize_database(
    engine: AsyncEngine,
    session_factory: async_sessionmaker[AsyncSession],
    settings: Settings,
) -> None:
    """Run the explicitly enabled startup database steps in safe order."""
    await check_database_connection(engine)
    logger.info("Database connection established.")

    if settings.AUTO_MIGRATE:
        logger.info("Applying existing Alembic migrations during startup.")
        from app.database.migration import upgrade_database

        migration_url = settings.MIGRATION_DATABASE_URL or settings.DATABASE_URL
        await asyncio.to_thread(upgrade_database, migration_url)

    missing = await get_missing_tables(engine)
    if missing and settings.AUTO_CREATE_TABLES:
        created = await create_missing_tables(engine)
        logger.info("Created missing database tables: %s", ", ".join(created))
        missing = await get_missing_tables(engine)

    if missing:
        logger.warning(
            "Database tables are missing: %s. Run `python -m app.database migrate` "
            "or explicitly enable AUTO_CREATE_TABLES for an unmanaged development database.",
            ", ".join(missing),
        )
        if settings.AUTO_SEED:
            raise RuntimeError(
                "AUTO_SEED=true requires all application tables to exist. "
                "Apply migrations or explicitly create tables first."
            )
    else:
        logger.info("All registered database tables exist.")

    if settings.AUTO_SEED:
        from app.database.seeders.runner import run_seeders

        completed = await run_seeders(session_factory)
        logger.info("Database seeders completed: %s", ", ".join(completed) or "none")


__all__ = [
    "check_database_connection",
    "create_missing_tables",
    "get_missing_tables",
    "initialize_database",
]
