"""Unit tests for DynamicServiceExecutor: Deep type-safe raw SQL, Redis caching, and Elasticsearch CQRS."""

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
import unittest.mock as mock
import uuid

import pytest
from pydantic import BaseModel

from app.core.cache_interface import ICacheService
from app.core.elasticsearch import IElasticsearchService
from app.db.pool import UnitOfWork
from app.models.outbox import OutboxEvent
from app.repositories.outbox import OutboxRepository
from app.services.executor import DynamicServiceExecutor, QueryIntent


class SampleUserRead(BaseModel):
    id: uuid.UUID
    name: str
    email: str
    role: str = "user"


@pytest.fixture
def mock_db() -> mock.AsyncMock:
    uow = mock.AsyncMock(spec=UnitOfWork)
    uow.fetchrow = mock.AsyncMock()
    uow.fetch = mock.AsyncMock()
    uow.fetchval = mock.AsyncMock()
    uow.execute = mock.AsyncMock()
    uow.commit = mock.AsyncMock()
    uow.rollback = mock.AsyncMock()
    return uow


@pytest.fixture
def mock_cache() -> mock.AsyncMock:
    cache = mock.AsyncMock(spec=ICacheService)
    cache.get_item.return_value = None
    cache.set_item.return_value = True
    cache.set_negative_cache.return_value = True
    cache.delete_item.return_value = True
    cache.invalidate_lists.return_value = True
    cache.get.return_value = None
    cache.set.return_value = True
    cache.delete.return_value = True
    return cache


@pytest.fixture
def mock_es() -> mock.AsyncMock:
    es = mock.AsyncMock(spec=IElasticsearchService)
    es.get_document.return_value = None
    es.index_document.return_value = True
    es.delete_document.return_value = True
    es.search.return_value = ([], 0)
    return es


@pytest.fixture
def mock_outbox_repo(mock_db: mock.AsyncMock) -> mock.AsyncMock:
    repo = mock.AsyncMock(spec=OutboxRepository)
    repo.create_event.return_value = OutboxEvent(
        id=uuid.uuid4(),
        aggregate_type="user",
        aggregate_id="123",
        event_type="CREATE",
        payload={"name": "Alice"},
        status="PENDING",
        retry_count=0,
        last_error=None,
        created_at=datetime.now(timezone.utc),
        published_at=None,
    )
    repo.mark_published.return_value = True
    return repo


@pytest.fixture
def executor(
    mock_db: mock.AsyncMock,
    mock_cache: mock.AsyncMock,
    mock_es: mock.AsyncMock,
    mock_outbox_repo: mock.AsyncMock,
) -> DynamicServiceExecutor:
    return DynamicServiceExecutor(
        db=mock_db,
        cache=mock_cache,
        elasticsearch=mock_es,
        outbox_repo=mock_outbox_repo,
    )


@pytest.mark.asyncio
async def test_execute_query_redis_l1_hit(
    executor: DynamicServiceExecutor,
    mock_db: mock.AsyncMock,
    mock_cache: mock.AsyncMock,
) -> None:
    """When key exists in Redis L1, returns cached data immediately without querying PostgreSQL."""
    user_id = uuid.uuid4()
    mock_cache.get.return_value = {
        "id": str(user_id),
        "name": "Sarah Connor",
        "email": "sarah@example.com",
        "role": "admin",
    }

    result = await executor.execute_query(
        sql="SELECT * FROM users WHERE id = $1",
        params=[user_id],
        fetch_mode="one",
        schema_cls=SampleUserRead,
        cache_key=f"users:item:{user_id}",
    )

    assert result is not None
    assert result.name == "Sarah Connor"
    assert result.email == "sarah@example.com"
    mock_cache.get.assert_awaited_once_with(f"users:item:{user_id}")
    mock_db.fetchrow.assert_not_called()


@pytest.mark.asyncio
async def test_execute_query_cache_miss_queries_db_and_auto_stores(
    executor: DynamicServiceExecutor,
    mock_db: mock.AsyncMock,
    mock_cache: mock.AsyncMock,
) -> None:
    """On cache miss, queries DB, auto-stores the query output in Redis, and returns typed schema."""
    user_id = uuid.uuid4()
    mock_cache.get.return_value = None
    mock_cache.get_item.return_value = None
    mock_db.fetchrow.return_value = {
        "id": user_id,
        "name": "John Connor",
        "email": "john@example.com",
        "role": "user",
    }

    result = await executor.execute_query(
        sql="SELECT * FROM users WHERE id = $1",
        params=[user_id],
        fetch_mode="one",
        schema_cls=SampleUserRead,
        cache_key=f"users:item:{user_id}",
    )

    assert result is not None
    assert result.name == "John Connor"
    mock_db.fetchrow.assert_awaited_once()
    mock_cache.set.assert_awaited_once()
    stored_call = mock_cache.set.call_args[0]
    assert stored_call[0] == f"users:item:{user_id}"
    assert stored_call[1]["name"] == "John Connor"


@pytest.mark.asyncio
async def test_execute_query_revalidate_bypasses_cache_and_refreshes(
    executor: DynamicServiceExecutor,
    mock_db: mock.AsyncMock,
    mock_cache: mock.AsyncMock,
) -> None:
    """When revalidate=True, skips reading Redis, executes DB query, and refreshes Redis."""
    user_id = uuid.uuid4()
    mock_db.fetchrow.return_value = {
        "id": user_id,
        "name": "Updated Name",
        "email": "updated@example.com",
        "role": "admin",
    }

    result = await executor.execute_query(
        sql="SELECT * FROM users WHERE id = $1",
        params=[user_id],
        fetch_mode="one",
        schema_cls=SampleUserRead,
        cache_key=f"users:item:{user_id}",
        revalidate=True,  # Forced refresh
    )

    assert result is not None
    assert result.name == "Updated Name"
    # Should NOT have attempted to read from Redis
    mock_cache.get.assert_not_called()
    mock_cache.get_item.assert_not_called()
    # Must have queried database
    mock_db.fetchrow.assert_awaited_once()
    # Must have refreshed Redis
    mock_cache.set.assert_awaited_once()


@pytest.mark.asyncio
async def test_execute_query_negative_caching(
    executor: DynamicServiceExecutor,
    mock_db: mock.AsyncMock,
    mock_cache: mock.AsyncMock,
) -> None:
    """When record does not exist in DB, sets negative cache sentinel in Redis."""
    user_id = uuid.uuid4()
    mock_cache.get.return_value = None
    mock_cache.get_item.return_value = None
    mock_db.fetchrow.return_value = None

    result = await executor.execute_query(
        sql="SELECT * FROM users WHERE id = $1",
        params=[user_id],
        fetch_mode="one",
        schema_cls=SampleUserRead,
        cache_key=f"users:item:{user_id}",
        negative_cache=True,
    )

    assert result is None
    mock_cache.set.assert_awaited_once_with(f"users:item:{user_id}", {"__not_found__": True}, ttl=30)


@pytest.mark.asyncio
async def test_execute_query_es_l2_hit_and_auto_backfill_redis(
    executor: DynamicServiceExecutor,
    mock_db: mock.AsyncMock,
    mock_cache: mock.AsyncMock,
    mock_es: mock.AsyncMock,
) -> None:
    """When key is missing in Redis but found in ES, returns document and backfills Redis."""
    user_id = uuid.uuid4()
    mock_cache.get.return_value = None
    mock_cache.get_item.return_value = None
    mock_es.get_document.return_value = {
        "id": str(user_id),
        "name": "ES Cached User",
        "email": "es@example.com",
        "role": "user",
    }

    result = await executor.execute_query(
        sql="SELECT * FROM users WHERE id = $1",
        params=[user_id],
        fetch_mode="one",
        schema_cls=SampleUserRead,
        cache_key=f"users:item:{user_id}",
        es_index="user",
        es_action="get",
        es_id=str(user_id),
    )

    assert result is not None
    assert result.name == "ES Cached User"
    mock_db.fetchrow.assert_not_called()
    # Backfills Redis
    mock_cache.set.assert_awaited_once_with(
        f"users:item:{user_id}",
        mock_es.get_document.return_value,
        ttl=None,
    )


@pytest.mark.asyncio
async def test_execute_write_stages_outbox_purges_cache_and_commits(
    executor: DynamicServiceExecutor,
    mock_db: mock.AsyncMock,
    mock_cache: mock.AsyncMock,
    mock_outbox_repo: mock.AsyncMock,
) -> None:
    """execute_write stages outbox event, purges Redis keys, bumps list version, and commits DB."""
    user_id = uuid.uuid4()
    mock_db.fetchrow.return_value = {
        "id": user_id,
        "name": "New User",
        "email": "new@example.com",
        "role": "admin",
    }

    result = await executor.execute_write(
        sql="INSERT INTO users (id, name, email) VALUES ($1, $2, $3) RETURNING *",
        params=[user_id, "New User", "new@example.com"],
        fetch_mode="one",
        schema_cls=SampleUserRead,
        cache_key=f"users:item:{user_id}",
        cache_action="set",
        extra_cache_keys=[f"users:profile:{user_id}"],
        invalidate_lists=True,
        es_index="user",
        es_action="index",
        es_id=str(user_id),
        use_outbox=True,
    )

    assert result is not None
    assert result.name == "New User"
    # 1. DB write executed
    mock_db.fetchrow.assert_awaited_once()
    # 2. Outbox event created
    mock_outbox_repo.create_event.assert_awaited_once()
    outbox_call = mock_outbox_repo.create_event.call_args[1]
    assert outbox_call["aggregate_type"] == "user"
    assert outbox_call["aggregate_id"] == str(user_id)
    assert outbox_call["event_type"] == "INDEX"
    # 3. Redis item set and profile purged
    mock_cache.set.assert_awaited_once()
    mock_cache.delete.assert_awaited_once_with(f"users:profile:{user_id}")
    # 4. Redis lists invalidated
    mock_cache.invalidate_lists.assert_awaited_once()
    # 5. DB committed
    mock_db.commit.assert_awaited_once()


@pytest.mark.asyncio
async def test_execute_search_with_es_and_sql_fallback(
    executor: DynamicServiceExecutor,
    mock_db: mock.AsyncMock,
    mock_es: mock.AsyncMock,
) -> None:
    """execute_search queries ES first; if ES returns 0 results, falls back to SQL query."""
    user_id = uuid.uuid4()
    # ES returns no hits
    mock_es.search.return_value = ([], 0)
    # DB returns matching rows
    mock_db.fetch.return_value = [
        {
            "id": user_id,
            "name": "Fallback User",
            "email": "fallback@example.com",
            "role": "user",
        }
    ]

    results = await executor.execute_search(
        es_index="user",
        es_query="Fallback",
        limit=20,
        offset=0,
        schema_cls=SampleUserRead,
        fallback_sql="SELECT * FROM users WHERE name ILIKE $1",
        fallback_params=["%Fallback%"],
    )

    assert len(results) == 1
    assert results[0].name == "Fallback User"
    mock_es.search.assert_awaited_once()
    mock_db.fetch.assert_awaited_once()
