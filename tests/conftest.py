"""Test setup: a fresh database per test, wired in by overriding get_session.

Default: SQLite file per test (fast, nothing to install). Foreign keys are switched on
so constraint behaviour matches MySQL/InnoDB.

Against real MySQL/MariaDB (use an EMPTY, throwaway database - tables are dropped):
    TEST_DATABASE_URL="mysql+aiomysql://user:pass@localhost:3306/app_test" pytest
"""

import os
from collections.abc import AsyncIterator

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy import event
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

from app.core.database import get_session
from app.main import create_app
from app.models import Base

TEST_DATABASE_URL = os.getenv("TEST_DATABASE_URL")


@pytest.fixture
async def session_factory(tmp_path) -> AsyncIterator[async_sessionmaker[AsyncSession]]:
    if TEST_DATABASE_URL:
        engine = create_async_engine(TEST_DATABASE_URL, poolclass=NullPool)
    else:
        engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'test.db'}")

        @event.listens_for(engine.sync_engine, "connect")
        def _enable_sqlite_fks(dbapi_conn, _):  # noqa: ANN001
            cursor = dbapi_conn.cursor()
            cursor.execute("PRAGMA foreign_keys=ON")
            cursor.close()

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)
    yield async_sessionmaker(engine, expire_on_commit=False, autoflush=False)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
    await engine.dispose()


@pytest.fixture
async def session(session_factory) -> AsyncIterator[AsyncSession]:
    async with session_factory() as s:
        yield s


@pytest.fixture
async def client(session_factory) -> AsyncIterator[AsyncClient]:
    app = create_app()

    async def _test_session() -> AsyncIterator[AsyncSession]:
        async with session_factory() as s:
            try:
                yield s
            except Exception:
                await s.rollback()
                raise

    app.dependency_overrides[get_session] = _test_session
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test/api/v1") as c:
        yield c


@pytest.fixture
async def category(client) -> dict:
    r = await client.post("/categories", json={"name": "Smartphones"})
    assert r.status_code == 201, r.text
    return r.json()


@pytest.fixture
async def products(client, category) -> list[dict]:
    rows = [
        {"name": "Galaxy A55", "sku": "sam-a55", "price": "38999.00", "stock": 12},
        {"name": "Galaxy S24", "sku": "sam-s24", "price": "99999.00", "stock": 3},
        {"name": "Redmi Note 13", "sku": "xia-rn13", "price": "24999.50", "stock": 0},
        {"name": "iPhone 15", "sku": "app-ip15", "price": "129999.00", "stock": 7},
        {"name": "Pixel 8a", "sku": "goo-p8a", "price": "54999.00", "stock": 2},
    ]
    out = []
    for row in rows:
        r = await client.post("/products", json={**row, "category_id": category["id"]})
        assert r.status_code == 201, r.text
        out.append(r.json())
    return out
