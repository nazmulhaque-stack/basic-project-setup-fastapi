"""Raw asyncpg connection pool management (no ORM, no SQLAlchemy).

PgBouncer transaction-pooling compatibility: `statement_cache_size=0` disables
asyncpg's per-connection prepared-statement cache, which is mandatory because
PgBouncer in transaction mode may hand a client a *different* physical backend
connection on the next transaction — a cached prepared statement name from the
previous backend would not exist on the new one.
"""

from collections.abc import AsyncGenerator
import re
from typing import Any, Optional

import asyncpg
from fastapi import Request
import orjson

from app.core.config import Settings, get_settings


async def _init_connection(connection: asyncpg.Connection) -> None:
    """Per-connection setup run by the pool for every new physical connection:
    transparently (de)serializes `json`/`jsonb` columns as Python objects via
    orjson (matching the rest of the codebase's JSON library), so repositories
    can pass/receive plain dicts instead of manually dumping/loading JSON text.
    """
    for pg_type in ("json", "jsonb"):
        await connection.set_type_codec(
            pg_type,
            encoder=lambda value: orjson.dumps(value).decode("utf-8"),
            decoder=orjson.loads,
            schema="pg_catalog",
            format="text",
        )


def normalize_asyncpg_dsn(database_url: str) -> str:
    """Strip a SQLAlchemy-style `+driver` dialect suffix (e.g. `postgresql+asyncpg://`)
    so the DSN is valid for asyncpg's own `connect()`/`create_pool()`, which only
    understands plain `postgres://` / `postgresql://`. Kept so existing `.env`
    values written for the old SQLAlchemy URL style don't need editing.
    """
    return re.sub(r"^(postgres(?:ql)?)\+[a-zA-Z0-9_]+://", r"\1://", database_url)


async def create_db_pool(settings: Optional[Settings] = None) -> asyncpg.Pool:
    """Creates the application-wide asyncpg connection pool.

    `min_size`/`max_size` map to the same `DB_POOL_SIZE`/`DB_MAX_OVERFLOW`
    settings the AGENTS.md connection-sizing formula is computed against.
    """
    settings = settings or get_settings()
    dsn = normalize_asyncpg_dsn(settings.DATABASE_URL)
    return await asyncpg.create_pool(
        dsn,
        min_size=settings.DB_POOL_SIZE,
        max_size=settings.DB_POOL_SIZE + settings.DB_MAX_OVERFLOW,
        max_inactive_connection_lifetime=settings.DB_POOL_RECYCLE,
        timeout=settings.DB_POOL_TIMEOUT,
        statement_cache_size=0,
        init=_init_connection,
    )


async def close_db_pool(pool: Optional[asyncpg.Pool]) -> None:
    """Gracefully closes the pool, waiting for in-flight connections to be released."""
    if pool is not None:
        await pool.close()


class UnitOfWork:
    """One acquired connection + one open transaction, exposed with explicit
    `commit()`/`rollback()` calls.

    `BaseService` was originally written against SQLAlchemy's `AsyncSession`
    (which has the same `commit()`/`rollback()` shape); this wrapper lets that
    service-layer transaction-boundary code work unchanged against asyncpg.
    Repositories only ever call `execute`/`fetch`/`fetchrow`/`fetchval` on this
    object — never `commit`/`rollback` — preserving the project's existing
    "repositories flush, services commit" rule (there is no separate "flush"
    step with asyncpg: statements run immediately inside the still-open
    transaction, and nothing is durable until `commit()`).
    """

    __slots__ = ("connection", "_transaction", "_closed")

    def __init__(self, connection: asyncpg.Connection) -> None:
        self.connection = connection
        self._transaction = connection.transaction()
        self._closed = False

    async def __aenter__(self) -> "UnitOfWork":
        await self._transaction.start()
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        if self._closed:
            return
        if exc_type is None:
            await self.commit()
        else:
            await self.rollback()

    async def commit(self) -> None:
        if self._closed:
            return
        await self._transaction.commit()
        self._closed = True

    async def rollback(self) -> None:
        if self._closed:
            return
        await self._transaction.rollback()
        self._closed = True

    async def fetch(self, query: str, *args: Any) -> list[asyncpg.Record]:
        return await self.connection.fetch(query, *args)

    async def fetchrow(self, query: str, *args: Any) -> Optional[asyncpg.Record]:
        return await self.connection.fetchrow(query, *args)

    async def fetchval(self, query: str, *args: Any) -> Any:
        return await self.connection.fetchval(query, *args)

    async def execute(self, query: str, *args: Any) -> str:
        return await self.connection.execute(query, *args)


async def get_pool(request: Request) -> asyncpg.Pool:
    """FastAPI dependency returning the application's asyncpg pool."""
    return request.app.state.db_pool


async def get_db(request: Request) -> AsyncGenerator[UnitOfWork, None]:
    """Acquires a pooled connection and opens a transaction for the request's
    lifetime. Commits automatically if the endpoint (and any explicit
    `uow.commit()` the service layer already issued) completed without error;
    rolls back on any unhandled exception. Same unit-of-work boundary the
    SQLAlchemy-based `get_db` provided — repositories still never commit.
    """
    pool: asyncpg.Pool = request.app.state.db_pool
    async with pool.acquire() as connection:
        async with UnitOfWork(connection) as uow:
            yield uow


# Alias matching fastapi-repo-pattern's existing naming
get_session = get_db

__all__ = [
    "UnitOfWork",
    "create_db_pool",
    "close_db_pool",
    "get_pool",
    "get_db",
    "get_session",
    "normalize_asyncpg_dsn",
]
