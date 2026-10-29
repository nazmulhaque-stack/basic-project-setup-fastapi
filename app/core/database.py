"""Async engine + session factory.

Transaction rule used in this project:
    Repositories only `flush()`. Services own the transaction and call `commit()`.

Why not commit inside the `get_session` dependency after `yield`?
    Since FastAPI 0.118, code after `yield` (default scope="request") runs AFTER the
    response has been sent. A commit that fails there would leave the client with a
    201/200 for data that was never saved. Committing in the service keeps the error
    inside the request, so the client gets a proper 409/500.
"""

from collections.abc import AsyncIterator

from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from app.core.config import settings


def build_engine(url: str = settings.DATABASE_URL) -> AsyncEngine:
    kwargs: dict = {"echo": settings.DB_ECHO, "pool_pre_ping": True}
    backend = make_url(url).get_backend_name()
    if backend != "sqlite":
        kwargs |= {
            "pool_size": settings.DB_POOL_SIZE,
            "max_overflow": settings.DB_MAX_OVERFLOW,
            "pool_recycle": settings.DB_POOL_RECYCLE,
        }
    if backend in {"mysql", "mariadb"}:
        # MySQL DATETIME has no timezone: make NOW() return UTC on every connection.
        kwargs["connect_args"] = {"init_command": "SET time_zone = '+00:00'"}
    return create_async_engine(url, **kwargs)


engine: AsyncEngine = build_engine()

# expire_on_commit=False is essential with AsyncSession: otherwise every attribute is
# expired after commit, and touching it later triggers implicit I/O -> MissingGreenlet.
SessionFactory = async_sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autoflush=False,
)


async def get_session() -> AsyncIterator[AsyncSession]:
    """One session per request. Closing without commit == rollback."""
    async with SessionFactory() as session:
        try:
            yield session
        except Exception:
            await session.rollback()
            raise
