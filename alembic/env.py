"""Alembic environment for asynchronous SQLAlchemy database migrations."""

import asyncio
import os

from alembic import context
from sqlalchemy import pool
from sqlalchemy.ext.asyncio import async_engine_from_config

from app.core.config import get_settings
from app.db.base import Base
from app.db.session import normalize_async_database_url

config = context.config
target_metadata = Base.metadata


def get_database_url() -> str:
    settings = get_settings()
    url = (
        os.getenv("MIGRATION_DATABASE_URL")
        or os.getenv("DATABASE_URL")
        or settings.MIGRATION_DATABASE_URL
        or settings.DATABASE_URL
    )
    return normalize_async_database_url(url)


def run_migrations_offline() -> None:
    context.configure(
        url=get_database_url(),
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        compare_type=True,
    )
    with context.begin_transaction():
        context.run_migrations()


def do_run_migrations(connection) -> None:
    dialect = connection.dialect.name
    lock_acquired = False
    try:
        if dialect == "postgresql":
            connection.exec_driver_sql(
                "SELECT pg_advisory_lock(738291046)"
            ).scalar_one()
            lock_acquired = True
        elif dialect == "mysql":
            acquired = connection.exec_driver_sql(
                "SELECT GET_LOCK('fastapi_schema_migrations', 60)"
            ).scalar_one()
            if acquired != 1:
                raise RuntimeError("Could not acquire the database migration lock.")
            lock_acquired = True

        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            compare_type=True,
        )
        with context.begin_transaction():
            context.run_migrations()
        connection.commit()
    finally:
        if lock_acquired and dialect == "postgresql":
            connection.exec_driver_sql(
                "SELECT pg_advisory_unlock(738291046)"
            ).scalar_one()
        elif lock_acquired and dialect == "mysql":
            connection.exec_driver_sql(
                "SELECT RELEASE_LOCK('fastapi_schema_migrations')"
            )


async def run_async_migrations() -> None:
    configuration = config.get_section(config.config_ini_section, {})
    url = get_database_url()
    configuration["sqlalchemy.url"] = url
    engine_kwargs = {
        "prefix": "sqlalchemy.",
        "poolclass": pool.NullPool,
    }
    if "asyncpg" in url:
        engine_kwargs["connect_args"] = {
            "statement_cache_size": 0,
            "prepared_statement_cache_size": 0,
        }
    connectable = async_engine_from_config(
        configuration,
        **engine_kwargs,
    )

    try:
        async with connectable.connect() as connection:
            await connection.run_sync(do_run_migrations)
            await connection.commit()
    finally:
        await connectable.dispose()


def run_migrations_online() -> None:
    asyncio.run(run_async_migrations())


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
