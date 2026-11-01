"""SQLAlchemy 2.0 Asynchronous Database Engine and Session Factory.

Compatible with PgBouncer transaction pooling, PostgreSQL (asyncpg), and MySQL (asyncmy).
Provides NullPool and statement_cache_size=0 options for 100k-scale PgBouncer pooling.
"""

from collections.abc import AsyncGenerator
from typing import Any, Dict
import uuid

from fastapi import Request
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.pool import NullPool

from app.core.config import Settings, get_settings


def normalize_async_database_url(database_url: str) -> str:
    """Translate common sync driver URLs to drivers supported by this async app."""
    url = make_url(database_url)
    driver = url.drivername
    async_drivers = {
        "postgres": "postgresql+asyncpg",
        "postgresql": "postgresql+asyncpg",
        "mysql": "mysql+asyncmy",
        "mysql+pymysql": "mysql+asyncmy",
        "mysql+mysqldb": "mysql+asyncmy",
        "sqlite": "sqlite+aiosqlite",
    }
    return url.set(drivername=async_drivers.get(driver, driver)).render_as_string(
        hide_password=False
    )


def create_db_engine(custom_settings: Settings | None = None) -> AsyncEngine:
    """Factory helper creating an async engine tuned for PgBouncer or direct database pooling."""
    custom_settings = custom_settings or get_settings()
    database_url = normalize_async_database_url(custom_settings.DATABASE_URL)
    connect_args: Dict[str, Any] = {}
    engine_kwargs: Dict[str, Any] = {
        "echo": custom_settings.DEBUG,
    }

    # If using PostgreSQL with asyncpg through PgBouncer transaction pooling:
    # 1. Disable client-side statement cache because PgBouncer transaction mode switches server connections
    # 2. Use NullPool so PgBouncer manages the 10,000+ client connections to 50 real server connections
    if "asyncpg" in database_url:
        if custom_settings.PGBOUNCER_ENABLED:
            connect_args.update({
                "statement_cache_size": 0,
                "prepared_statement_cache_size": 0,
                # Prevent name collisions when PgBouncer reuses backend connections.
                "prepared_statement_name_func": lambda: f"__asyncpg_{uuid.uuid4().hex}__",
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
    elif "sqlite" in database_url:
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

    return create_async_engine(database_url, **engine_kwargs)


def create_session_factory(db_engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    """Factory helper to create an async sessionmaker bound to an engine."""
    return async_sessionmaker(
        bind=db_engine,
        class_=AsyncSession,
        expire_on_commit=False,
        autoflush=False,
    )


async def get_db(request: Request) -> AsyncGenerator[AsyncSession, None]:
    """Yield a session from the engine owned by this application lifespan."""
    session_factory: async_sessionmaker[AsyncSession] = request.app.state.session_factory
    async with session_factory() as session:
        yield session


# Alias matching fastapi-repo-pattern
get_session = get_db

__all__ = [
    "get_db",
    "get_session",
    "create_db_engine",
    "create_session_factory",
    "normalize_async_database_url",
]
