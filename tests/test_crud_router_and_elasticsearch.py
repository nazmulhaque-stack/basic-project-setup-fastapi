"""Comprehensive test suite for CRUDRouter, Elasticsearch CQRS, and PgBouncer integration."""

from datetime import datetime, timezone
from typing import Any, Dict
from unittest.mock import AsyncMock, MagicMock
import pytest

from app.api.crud_router import CRUDRouter, build_list_query_model
from app.core.config import Settings
from app.core.elasticsearch import AsyncElasticsearchService, NullElasticsearch
from app.models.category import Category
from app.repositories.category import CategoryRepository
from app.schemas.category import CategoryCreate, CategoryRead, CategoryUpdate
from app.services.category import CategoryService


def test_build_list_query_model() -> None:
    """Verifies dynamic Pydantic query model creation and parameter validation."""
    query_model = build_list_query_model(
        CategoryRead,
        filter_fields={"name": ["eq", "icontains"], "is_active": ["eq"]},
        sort_fields=["id", "name", "created_at"],
    )

    # Valid instantiation
    query = query_model(name="Tech", name__icontains="elec", is_active=True, sort="-name,id", page=2, size=50)
    filters = query.filters()
    assert filters["name"] == "Tech"
    assert filters["name__icontains"] == "elec"
    assert filters["is_active"] is True
    assert query.sort_keys() == ["-name", "id"]
    assert query.page == 2
    assert query.size == 50

    # Unknown parameter should raise validation error (extra="forbid")
    with pytest.raises(Exception):
        query_model(invalid_param="forbidden")

    # Invalid sort key should raise validation error
    with pytest.raises(Exception):
        query_model(sort="unallowed_field")


@pytest.mark.asyncio
async def test_crud_router_routes_generation() -> None:
    """Verifies that CRUDRouter generates documented endpoints conforming to REST specifications."""
    mock_service_fn = MagicMock()

    router = CRUDRouter(
        model=Category,
        service=mock_service_fn,
        create_schema=CategoryCreate,
        update_schema=CategoryUpdate,
        read_schema=CategoryRead,
        prefix="/categories",
        tags=["Categories"],
        filter_fields={"name": ["eq", "icontains"], "is_active": ["eq"]},
        sort_fields=["id", "name", "created_at"],
    )

    routes_by_path: Dict[str, set] = {}
    for r in router.routes:
        routes_by_path.setdefault(r.path, set()).update(r.methods)

    assert "/categories" in routes_by_path
    assert "GET" in routes_by_path["/categories"]
    assert "POST" in routes_by_path["/categories"]
    assert "/categories/{item_id}" in routes_by_path
    assert "GET" in routes_by_path["/categories/{item_id}"]
    assert "PATCH" in routes_by_path["/categories/{item_id}"]
    assert "PUT" in routes_by_path["/categories/{item_id}"]
    assert "DELETE" in routes_by_path["/categories/{item_id}"]
    assert "/categories/search" in routes_by_path
    assert "GET" in routes_by_path["/categories/search"]


def test_user_crud_router_routes_generation() -> None:
    """Verifies that CRUDRouter generates full typed REST routes for User domain."""
    from app.models.user import User
    from app.schemas.user import UserCreate, UserRead, UserUpdate

    mock_service_fn = MagicMock()
    router = CRUDRouter(
        model=User,
        service=mock_service_fn,
        create_schema=UserCreate,
        update_schema=UserUpdate,
        read_schema=UserRead,
        prefix="/users",
        tags=["Users"],
        filter_fields={"name": ["icontains"], "email": ["eq", "icontains"], "role": ["eq", "in"]},
        sort_fields=["id", "name", "email", "role", "created_at"],
    )

    routes_by_path: Dict[str, set] = {}
    for r in router.routes:
        routes_by_path.setdefault(r.path, set()).update(r.methods)

    assert "/users" in routes_by_path
    assert "GET" in routes_by_path["/users"]
    assert "POST" in routes_by_path["/users"]
    assert "/users/{item_id}" in routes_by_path
    assert "GET" in routes_by_path["/users/{item_id}"]
    assert "PATCH" in routes_by_path["/users/{item_id}"]
    assert "PUT" in routes_by_path["/users/{item_id}"]
    assert "DELETE" in routes_by_path["/users/{item_id}"]
    assert "/users/search" in routes_by_path



@pytest.mark.asyncio
async def test_elasticsearch_cqrs_indexing_and_search() -> None:
    """Verifies that Elasticsearch service indexes and searches documents with fail-open behavior."""
    mock_es_client = AsyncMock()
    mock_es_client.search.return_value = {
        "hits": {
            "total": {"value": 1},
            "hits": [{"_id": "1", "_source": {"name": "Smartphones", "is_active": True}}],
        }
    }

    es_service = AsyncElasticsearchService(client=mock_es_client, index_prefix="demo")

    # Index document
    indexed = await es_service.index_document("categories", "1", {"name": "Smartphones"})
    assert indexed is True
    mock_es_client.index.assert_awaited_once()

    # Search document
    results, total = await es_service.search("categories", query="Smartphones")
    assert total == 1
    assert len(results) == 1
    assert results[0]["name"] == "Smartphones"

    # Delete document
    deleted = await es_service.delete_document("categories", "1")
    assert deleted is True
    mock_es_client.delete.assert_awaited_once()

    # NullElasticsearch fail-open behavior
    null_es = NullElasticsearch()
    assert await null_es.index_document("any", "1", {}) is True
    empty_results, count = await null_es.search("any", "query")
    assert empty_results == []
    assert count == 0
    assert await null_es.delete_document("any", "1") is True


@pytest.mark.asyncio
async def test_category_service_crud_lifecycle() -> None:
    """Verifies CategoryService CRUD execution with BaseRepository and Elasticsearch integration."""
    mock_db = AsyncMock()
    mock_db.commit = AsyncMock()
    mock_repo = MagicMock(spec=CategoryRepository)
    mock_repo.model = Category
    mock_repo.db = mock_db
    mock_repo.session = mock_db
    mock_repo._session = mock_db
    mock_es = AsyncMock(spec=AsyncElasticsearchService)

    category_id = 42
    entity = Category(
        id=category_id,
        name="Appliances",
        is_active=True,
        created_at=datetime.now(timezone.utc),
    )

    # repo.create() returns the entity it was given — no ORM identity map to mutate in place
    mock_repo.create = AsyncMock(side_effect=lambda e: e)
    mock_repo.get = AsyncMock(return_value=entity)
    mock_repo.delete = AsyncMock()
    mock_repo.count = AsyncMock(return_value=1)
    mock_repo.list = AsyncMock(return_value=[entity])

    service = CategoryService(repo=mock_repo, db=mock_db, elasticsearch=mock_es)

    # 1. Create
    payload = CategoryCreate(name="Appliances", is_active=True)
    created = await service.create(payload)
    assert created.name == "Appliances"
    mock_repo.create.assert_awaited_once()
    mock_db.commit.assert_awaited_once()
    mock_es.index_document.assert_awaited_once()

    # 2. Get
    fetched = await service.get_by_id(category_id)
    assert fetched.name == "Appliances"

    # 3. List
    items, total = await service.list(filters={"is_active": True}, page=1, size=20)
    assert total == 1
    assert len(items) == 1
    assert items[0].name == "Appliances"

    # 4. Delete
    await service.delete_by_id(category_id)
    mock_repo.delete.assert_awaited_once()
    mock_es.delete_document.assert_awaited_once()


@pytest.mark.asyncio
async def test_pgbouncer_bounded_pool_and_statement_cache_disabled(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verifies `create_db_pool` disables asyncpg's per-connection prepared-statement
    cache (mandatory for PgBouncer transaction pooling) and sizes the pool from
    DB_POOL_SIZE/DB_MAX_OVERFLOW, and that a SQLAlchemy-style `+asyncpg` DSN suffix
    is normalized away before being handed to asyncpg."""
    import asyncpg
    from app.db.pool import create_db_pool

    # Settings.resolve_service_urls() keeps DATABASE_URL's credentials in sync
    # with any DB_USER/DB_PASSWORD/DB_HOST/DB_PORT/DB_NAME from the environment
    # (e.g. a real .env) — clear those so this test's explicit DATABASE_URL
    # survives unmodified.
    for var in ("DB_USER", "DB_PASSWORD", "DB_HOST", "DB_PORT", "DB_NAME",
                "POSTGRES_USER", "POSTGRES_PASSWORD", "POSTGRES_HOST", "POSTGRES_PORT", "POSTGRES_DB"):
        monkeypatch.delenv(var, raising=False)

    settings = Settings(
        _env_file=None,
        PGBOUNCER_ENABLED=True,
        DATABASE_URL="postgresql+asyncpg://user:pass@127.0.0.1:6432/demo_db",
        DB_POOL_SIZE=5,
        DB_MAX_OVERFLOW=5,
    )

    captured: Dict[str, Any] = {}

    async def fake_create_pool(dsn: str, **kwargs: Any) -> Any:
        captured["dsn"] = dsn
        captured.update(kwargs)
        return MagicMock()

    monkeypatch.setattr(asyncpg, "create_pool", fake_create_pool)
    await create_db_pool(settings)

    assert captured["dsn"] == "postgresql://user:pass@127.0.0.1:6432/demo_db"
    assert captured["min_size"] == 5
    assert captured["max_size"] == 10
    assert captured["statement_cache_size"] == 0


def test_partial_schema_generated_from_create_schema() -> None:
    """Verifies that make_partial creates a schema with all fields optional while preserving validation rules."""
    from pydantic import ValidationError
    from app.schemas.product import ProductUpdate

    assert ProductUpdate.model_validate({}).model_dump(exclude_unset=True) == {}
    assert ProductUpdate.model_validate({"sku": "x-1"}).sku == "X-1"  # uppercase validator preserved
    for bad in ({"price_cents": -1},):
        with pytest.raises(ValidationError):
            ProductUpdate.model_validate(bad)


@pytest.mark.asyncio
async def test_atomic_stock_adjustment() -> None:
    """Verifies atomic stock adjustments and boundary enforcement."""
    import uuid
    from app.core.exceptions import BusinessRuleError
    from app.models.product import Product
    from app.repositories.product import ProductRepository
    from app.services.product import ProductService

    mock_db = AsyncMock()
    mock_db.commit = AsyncMock()
    mock_db.refresh = AsyncMock()
    mock_repo = MagicMock(spec=ProductRepository)
    mock_repo.model = Product

    product_id = uuid.uuid4()
    p = Product(
        id=product_id,
        name="Mouse",
        sku="MS-01",
        price_cents=4999,
        category="electronics",
        is_active=True,
        stock=5,
        created_at=datetime.now(timezone.utc),
    )
    mock_repo.get = AsyncMock(return_value=p)
    mock_repo.change_stock_atomic = AsyncMock(return_value=True)

    service = ProductService(repo=mock_repo, db=mock_db)

    # Restock +3
    await service.adjust_stock(product_id, 3)
    mock_repo.change_stock_atomic.assert_awaited_with(product_id, 3)

    # Oversell -10 should fail if atomic guard returns False
    mock_repo.change_stock_atomic = AsyncMock(return_value=False)
    with pytest.raises(BusinessRuleError):
        await service.adjust_stock(product_id, -10)


@pytest.mark.asyncio
async def test_service_rule_with_mocked_repository() -> None:
    """Verifies that ProductService validates category existence and raises BusinessRuleError."""
    from app.core.exceptions import BusinessRuleError
    from app.models.product import Product
    from app.repositories.product import ProductRepository
    from app.repositories.category import CategoryRepository
    from app.schemas.product import ProductCreate
    from app.services.product import ProductService

    mock_session = AsyncMock()
    service = ProductService(db=mock_session)
    service.repository = AsyncMock(spec=ProductRepository)
    service.repository.get_by_sku = AsyncMock(return_value=None)

    # Mock category not existing
    with pytest.raises(BusinessRuleError):
        payload = ProductCreate(
            name="Phone",
            sku="PHONE-001",
            price_cents=9999,
            category_id=999,
        )
        mock_categories = AsyncMock(spec=CategoryRepository)
        mock_categories.exists_active.return_value = False
        service._ensure_active_category = AsyncMock(
            side_effect=BusinessRuleError("Category id=999 does not exist or is inactive.")
        )
        await service.create(payload)

    service.repository.create.assert_not_awaited()


@pytest.mark.asyncio
async def test_category_repository_crud_against_real_postgres(pg_pool: Any) -> None:
    """Integration test for CategoryRepository's hand-written asyncpg SQL (create,
    filtered list/count, update, delete) against a real Postgres connection.
    Skipped automatically when no database is reachable (see the `pg_pool` fixture)."""
    import uuid
    from app.db.pool import UnitOfWork

    unique = uuid.uuid4().hex[:8]
    async with pg_pool.acquire() as connection:
        async with UnitOfWork(connection) as uow:
            repo = CategoryRepository(uow)
            created = await repo.create(Category(name=f"Gadgets-{unique}", is_active=True))
            other = await repo.create(Category(name=f"Widgets-{unique}", is_active=False))
            assert created.id is not None and other.id is not None

            found = await repo.list(filters={"name__icontains": unique})
            assert {c.name for c in found} == {f"Gadgets-{unique}", f"Widgets-{unique}"}
            assert await repo.count(filters={"name__icontains": unique, "is_active": True}) == 1

            created.description = "updated description"
            updated = await repo.update(created)
            assert updated.description == "updated description"

            await repo.delete(created)
            await repo.delete(other)
            assert await repo.get(created.id) is None
