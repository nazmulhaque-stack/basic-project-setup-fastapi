"""SQLAlchemy 2.0 Asynchronous Database Engine and Session Factory.

Compatible with PgBouncer transaction pooling, PostgreSQL (asyncpg), and MySQL (asyncmy).
Provides NullPool and statement_cache_size=0 options for 100k-scale PgBouncer pooling.
"""

from collections.abc import AsyncGenerator
from typing import Any, Dict
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.pool import NullPool

from app.core.config import Settings, get_settings

settings: Settings = get_settings()


def create_db_engine(custom_settings: Settings = settings) -> AsyncEngine:
    """Factory helper creating an async engine tuned for PgBouncer or direct database pooling."""
    connect_args: Dict[str, Any] = {}
    engine_kwargs: Dict[str, Any] = {
        "echo": custom_settings.DEBUG,
    }

    # If using PostgreSQL with asyncpg through PgBouncer transaction pooling:
    # 1. Disable client-side statement cache because PgBouncer transaction mode switches server connections
    # 2. Use NullPool so PgBouncer manages the 10,000+ client connections to 50 real server connections
    if "asyncpg" in custom_settings.DATABASE_URL:
        if custom_settings.PGBOUNCER_ENABLED:
            connect_args.update({
                "statement_cache_size": 0,
                "prepared_statement_cache_size": 0,
            })
            engine_kwargs["poolclass"] = NullPool
        else:
            engine_kwargs.update({
                "pool_size": custom_settings.DB_POOL_SIZE,
                "max_overflow": custom_settings.DB_MAX_OVERFLOW,
                "pool_timeout": custom_settings.DB_POOL_TIMEOUT,
                "pool_recycle": custom_settings.DB_POOL_RECYCLE,
                "pool_pre_ping": True,
            })
    elif "sqlite" in custom_settings.DATABASE_URL:
        engine_kwargs["poolclass"] = NullPool
    else:
        engine_kwargs.update({
            "pool_size": custom_settings.DB_POOL_SIZE,
            "max_overflow": custom_settings.DB_MAX_OVERFLOW,
            "pool_timeout": custom_settings.DB_POOL_TIMEOUT,
            "pool_recycle": custom_settings.DB_POOL_RECYCLE,
            "pool_pre_ping": True,
        })

    if connect_args:
        engine_kwargs["connect_args"] = connect_args

    return create_async_engine(custom_settings.DATABASE_URL, **engine_kwargs)


engine: AsyncEngine = create_db_engine(settings)

AsyncSessionFactory: async_sessionmaker[AsyncSession] = async_sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autoflush=False,
)


def create_session_factory(db_engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    """Factory helper to create an async sessionmaker bound to an engine."""
    return async_sessionmaker(
        bind=db_engine,
        class_=AsyncSession,
        expire_on_commit=False,
        autoflush=False,
    )


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """Dependency generating an asynchronous SQLAlchemy session with automatic cleanup."""
    async with AsyncSessionFactory() as session:
        yield session


# Alias matching fastapi-repo-pattern
get_session = get_db

__all__ = [
    "engine",
    "AsyncSessionFactory",
    "get_db",
    "get_session",
    "create_db_engine",
    "create_session_factory",
]
