"""Transactional execution of registered seeders."""

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.database.seeders.registry import get_registered_seeders, order_seeders


def load_seeders() -> None:
    """Import built-in seeder modules so their explicit registrations run."""
    from app.database.seeders import category_seeder, user_seeder  # noqa: F401


async def run_seeders(
    session_factory: async_sessionmaker[AsyncSession],
    requested: list[str] | None = None,
) -> list[str]:
    load_seeders()
    ordered = order_seeders(get_registered_seeders(), requested)
    async with session_factory() as session:
        async with session.begin():
            dialect = session.get_bind().dialect.name
            mysql_lock_acquired = False
            if dialect == "postgresql":
                await session.execute(
                    text("SELECT pg_advisory_xact_lock(:key)"),
                    {"key": 738291048},
                )
            elif dialect == "mysql":
                acquired = await session.scalar(
                    text("SELECT GET_LOCK('fastapi_database_seeders', 60)")
                )
                if acquired != 1:
                    raise RuntimeError("Could not acquire the database seeder lock.")
                mysql_lock_acquired = True
            try:
                for seeder_class in ordered:
                    await seeder_class().seed(session)
            finally:
                if mysql_lock_acquired:
                    await session.execute(
                        text("SELECT RELEASE_LOCK('fastapi_database_seeders')")
                    )
    return [seeder_class.name for seeder_class in ordered]


__all__ = ["load_seeders", "run_seeders"]
