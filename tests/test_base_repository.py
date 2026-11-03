"""Unit tests for BaseRepository dynamic execution engine.

Tests:
1. fetch_one cache hit and miss
2. fetch_one cache=False bypasses Redis
3. insert triggers _enqueue_es_sync when table registered
4. insert with auto_sync=False skips ES sync
5. update_row invalidates cache
6. delete_row hard delete
7. delete_row soft delete
8. execute with entity_id triggers ES sync
9. execute_many invalidates cache
10. bulk_sync enqueues ES events
11. search delegates to elasticsearch.search
12. search with search_enabled=False returns empty
"""

from typing import Any, Dict, List, Optional
import unittest.mock as mock
import uuid
import pytest

from app.core.elasticsearch import IElasticsearchService
from app.db.pool import UnitOfWork
from app.models.base import Base
from app.repositories.base import BaseRepository, ES_SYNC_TOPIC, SQLExpr
from app.services.interface import ICacheService


class MockModel(Base):
    id: uuid.UUID
    name: str


class ConcreteTestRepository(BaseRepository[MockModel, uuid.UUID]):
    table = "products"
    primary_key = "id"
    cache_prefix = "products"

    @property
    def model(self) -> type[MockModel]:
        return MockModel


@pytest.fixture
def mock_db() -> mock.AsyncMock:
    uow = mock.AsyncMock(spec=UnitOfWork)
    uow.connection = mock.AsyncMock()
    return uow


@pytest.fixture
def mock_cache() -> mock.AsyncMock:
    cache = mock.AsyncMock(spec=ICacheService)
    cache.get.return_value = None
    cache.set.return_value = True
    cache.incr.return_value = 2
    return cache


@pytest.fixture
def mock_es() -> mock.AsyncMock:
    es = mock.AsyncMock(spec=IElasticsearchService)
    es.search.return_value = ([{"id": "1", "name": "Test Product"}], 1)
    return es


@pytest.fixture
def repo(mock_db: mock.AsyncMock, mock_cache: mock.AsyncMock, mock_es: mock.AsyncMock) -> ConcreteTestRepository:
    return ConcreteTestRepository(
        db=mock_db,
        cache=mock_cache,
        elasticsearch=mock_es,
    )


@pytest.mark.asyncio
async def test_fetch_one_cache_miss_and_store(repo: ConcreteTestRepository, mock_db: mock.AsyncMock, mock_cache: mock.AsyncMock) -> None:
    """fetch_one with cache=True: on miss, queries DB and caches result."""
    fake_row = {"id": uuid.uuid4(), "name": "Widget"}
    mock_db.fetchrow.return_value = fake_row
    mock_cache.get.return_value = None

    result = await repo.fetch_one(
        "SELECT * FROM products WHERE id = $1",
        (fake_row["id"],),
        cache=True,
        cache_ttl=300,
    )

    assert result == fake_row
    mock_db.fetchrow.assert_awaited_once()
    mock_cache.get.assert_awaited_once()
    mock_cache.set.assert_awaited_once()


@pytest.mark.asyncio
async def test_fetch_one_cache_hit(repo: ConcreteTestRepository, mock_db: mock.AsyncMock, mock_cache: mock.AsyncMock) -> None:
    """fetch_one with cache=True: on hit, returns cached data without querying DB."""
    fake_row = {"id": str(uuid.uuid4()), "name": "Cached Widget"}
    mock_cache.get.return_value = {"row": fake_row}

    result = await repo.fetch_one(
        "SELECT * FROM products WHERE id = $1",
        ("some-id",),
        cache=True,
    )

    assert result == fake_row
    mock_db.fetchrow.assert_not_awaited()


@pytest.mark.asyncio
async def test_fetch_one_cache_disabled(repo: ConcreteTestRepository, mock_db: mock.AsyncMock, mock_cache: mock.AsyncMock) -> None:
    """fetch_one with cache=False (default): bypasses cache completely."""
    fake_row = {"id": uuid.uuid4(), "name": "Direct DB Widget"}
    mock_db.fetchrow.return_value = fake_row

    result = await repo.fetch_one(
        "SELECT * FROM products WHERE id = $1",
        (fake_row["id"],),
        cache=False,
    )

    assert result == fake_row
    mock_db.fetchrow.assert_awaited_once()
    mock_cache.get.assert_not_awaited()
    mock_cache.set.assert_not_awaited()


@pytest.mark.asyncio
async def test_insert_auto_sync_enqueues_es_outbox(repo: ConcreteTestRepository, mock_db: mock.AsyncMock, mock_cache: mock.AsyncMock) -> None:
    """insert with auto_sync=True stages an outbox_events row in the same transaction."""
    prod_id = uuid.uuid4()
    mock_db.fetchrow.return_value = {"id": prod_id, "name": "New Item"}

    with mock.patch("app.repositories.base.get_index_config") as mock_cfg:
        mock_cfg.return_value = {"index": "product", "primary_key": "id", "enabled": True, "auto_sync": True}

        result = await repo.insert(
            {"id": prod_id, "name": "New Item"},
            auto_sync=True,
        )

        assert result["id"] == prod_id
        # Cache invalidated
        mock_cache.incr.assert_awaited_once()
        # Outbox event staged
        mock_db.execute.assert_awaited_once()
        call_args = mock_db.execute.call_args[0]
        assert "INSERT INTO outbox_events" in call_args[0]
        assert call_args[2] == ES_SYNC_TOPIC
        assert call_args[3] == str(prod_id)
        assert call_args[4] == "es_sync.products.upsert"


@pytest.mark.asyncio
async def test_insert_skip_es_sync(repo: ConcreteTestRepository, mock_db: mock.AsyncMock) -> None:
    """insert with auto_sync=False does not stage an outbox event."""
    prod_id = uuid.uuid4()
    mock_db.fetchrow.return_value = {"id": prod_id, "name": "Silent Item"}

    await repo.insert(
        {"id": prod_id, "name": "Silent Item"},
        auto_sync=False,
    )

    # Only the fetchrow for INSERT, no execute for outbox_events
    mock_db.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_update_row_invalidates_cache_and_syncs(repo: ConcreteTestRepository, mock_db: mock.AsyncMock, mock_cache: mock.AsyncMock) -> None:
    """update_row invalidates the query cache and syncs updated row to ES outbox."""
    prod_id = uuid.uuid4()
    mock_db.fetchrow.return_value = {"id": prod_id, "name": "Updated Item"}

    with mock.patch("app.repositories.base.get_index_config") as mock_cfg:
        mock_cfg.return_value = {"index": "product", "primary_key": "id", "enabled": True, "auto_sync": True}

        result = await repo.update_row(
            {"name": "Updated Item", "updated_at": SQLExpr("now()")},
            {"id": prod_id},
            auto_sync=True,
        )

        assert result is not None
        assert result["name"] == "Updated Item"
        mock_cache.incr.assert_awaited_once()
        mock_db.execute.assert_awaited_once()
        assert "INSERT INTO outbox_events" in mock_db.execute.call_args[0][0]


@pytest.mark.asyncio
async def test_delete_row_hard_delete(repo: ConcreteTestRepository, mock_db: mock.AsyncMock, mock_cache: mock.AsyncMock) -> None:
    """delete_row with soft_delete=False emits DELETE and stages delete sync event."""
    prod_id = uuid.uuid4()
    mock_db.fetch.return_value = [{"id": prod_id}]

    with mock.patch("app.repositories.base.get_index_config") as mock_cfg:
        mock_cfg.return_value = {"index": "product", "primary_key": "id", "enabled": True, "auto_sync": True}

        deleted_count = await repo.delete_row({"id": prod_id}, auto_sync=True, soft_delete=False)

        assert deleted_count == 1
        mock_cache.incr.assert_awaited_once()
        call_args = mock_db.execute.call_args[0]
        assert "es_sync.products.delete" in call_args[4]


@pytest.mark.asyncio
async def test_delete_row_soft_delete(repo: ConcreteTestRepository, mock_db: mock.AsyncMock, mock_cache: mock.AsyncMock) -> None:
    """delete_row with soft_delete=True emits UPDATE and stages upsert sync event."""
    prod_id = uuid.uuid4()
    mock_db.fetch.return_value = [{"id": prod_id, "is_active": False}]

    with mock.patch("app.repositories.base.get_index_config") as mock_cfg:
        mock_cfg.return_value = {"index": "product", "primary_key": "id", "enabled": True, "auto_sync": True}

        deleted_count = await repo.delete_row(
            {"id": prod_id},
            auto_sync=True,
            soft_delete=True,
            soft_delete_column="is_active",
        )

        assert deleted_count == 1
        mock_cache.incr.assert_awaited_once()
        call_args = mock_db.execute.call_args[0]
        # Soft-delete is an upsert in ES (updating is_active to false)
        assert "es_sync.products.upsert" in call_args[4]


@pytest.mark.asyncio
async def test_execute_with_entity_id_triggers_es_sync(repo: ConcreteTestRepository, mock_db: mock.AsyncMock) -> None:
    """execute() with auto_sync=True and explicit entity_id fetches row and stages sync."""
    prod_id = uuid.uuid4()
    mock_db.execute.side_effect = ["UPDATE 1", "INSERT 0 1"]
    mock_db.fetchrow.return_value = {"id": prod_id, "stock": 15}

    with mock.patch("app.repositories.base.get_index_config") as mock_cfg:
        mock_cfg.return_value = {"index": "product", "primary_key": "id", "enabled": True, "auto_sync": True}

        status = await repo.execute(
            "UPDATE products SET stock = stock + $2 WHERE id = $1",
            (prod_id, 5),
            auto_sync=True,
            entity_id=prod_id,
        )

        assert status == "UPDATE 1"
        assert mock_db.execute.await_count == 2


@pytest.mark.asyncio
async def test_execute_many_invalidates_cache(repo: ConcreteTestRepository, mock_db: mock.AsyncMock, mock_cache: mock.AsyncMock) -> None:
    """execute_many invalidates the query cache after bulk execution."""
    records = [(uuid.uuid4(), "A"), (uuid.uuid4(), "B")]

    await repo.execute_many(
        "INSERT INTO products (id, name) VALUES ($1, $2)",
        records,
    )

    mock_db.connection.executemany.assert_awaited_once()
    mock_cache.incr.assert_awaited_once()


@pytest.mark.asyncio
async def test_bulk_sync_enqueues_es_events(repo: ConcreteTestRepository, mock_db: mock.AsyncMock) -> None:
    """bulk_sync stages outbox events for multiple entity IDs."""
    ids = [uuid.uuid4(), uuid.uuid4(), uuid.uuid4()]

    with mock.patch("app.repositories.base.get_index_config") as mock_cfg:
        mock_cfg.return_value = {"index": "product", "primary_key": "id", "enabled": True, "auto_sync": True}

        await repo.bulk_sync("upsert", ids)

        assert mock_db.execute.await_count == 3


@pytest.mark.asyncio
async def test_search_delegates_to_elasticsearch(repo: ConcreteTestRepository, mock_es: mock.AsyncMock) -> None:
    """search() forwards query to elasticsearch service when search_enabled=True."""
    dsl_query = {"match": {"name": "Widget"}}

    hits, total = await repo.search(dsl_query, search_enabled=True, limit=10)

    assert total == 1
    assert len(hits) == 1
    mock_es.search.assert_awaited_once_with(
        index="products",
        query=dsl_query,
        limit=10,
        offset=0,
    )


@pytest.mark.asyncio
async def test_search_disabled_returns_empty(repo: ConcreteTestRepository, mock_es: mock.AsyncMock) -> None:
    """search() with search_enabled=False short-circuits without calling Elasticsearch."""
    hits, total = await repo.search({"match_all": {}}, search_enabled=False)

    assert hits == []
    assert total == 0
    mock_es.search.assert_not_awaited()

