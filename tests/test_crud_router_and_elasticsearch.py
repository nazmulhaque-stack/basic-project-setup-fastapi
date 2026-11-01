"""Comprehensive test suite for CRUDRouter, Elasticsearch CQRS, and PgBouncer integration."""

from datetime import datetime, timezone
from typing import Any, Dict
from unittest.mock import AsyncMock, MagicMock
import pytest
from sqlalchemy.pool import NullPool

from app.api.crud_router import CRUDRouter, build_list_query_model
from app.core.config import Settings
from app.core.elasticsearch import AsyncElasticsearchService, NullElasticsearch
from app.db.session import create_db_engine
from app.models.category import Category
from app.repositories.category import CategoryRepository
from app.schemas.category import CategoryCreate, CategoryRead, CategoryUpdate
from app.services.category import CategoryService


def test_build_list_query_model() -> None:
    """Verifies dynamic Pydantic query model creation and parameter validation."""
    query_model = build_list_query_model(
        Category,
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

    mock_repo.create = AsyncMock()
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


def test_pgbouncer_nullpool_configuration(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verifies that engine applies NullPool and disables statement caching when PgBouncer is enabled."""
    settings = Settings(
        PGBOUNCER_ENABLED=True,
        DATABASE_URL="postgresql+asyncpg://user:pass@127.0.0.1:6432/demo_db",
    )
    import app.db.session as db_session

    engine_kwargs: Dict[str, Any] = {}
    original_create_async_engine = db_session.create_async_engine

    def capture_engine_kwargs(url: Any, **kwargs: Any) -> Any:
        engine_kwargs.update(kwargs)
        return original_create_async_engine(url, **kwargs)

    monkeypatch.setattr(db_session, "create_async_engine", capture_engine_kwargs)
    engine = create_db_engine(settings)
    assert isinstance(engine.pool, NullPool)
    connect_args = engine_kwargs["connect_args"]
    assert connect_args["statement_cache_size"] == 0
    assert connect_args["prepared_statement_cache_size"] == 0
    assert callable(connect_args["prepared_statement_name_func"])


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
async def test_repository_generic_crud_in_memory() -> None:
    """Verifies repository generic CRUD operations, default sorting, operators, and safety guards."""
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
    from app.core.exceptions import InvalidQueryError
    from app.db.base import Base

    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    async with session_factory() as session:
        repo = CategoryRepository(session)
        assert repo.model.__name__ == "Category"  # Inferred from CategoryRepository[Category]

        created = await repo.create_many([
            {"name": "B"},
            {"name": "A"},
            {"name": "C", "is_active": False},
        ])
        await session.commit()
        assert all(c.id for c in created)

        # list() uses default_sort = ("name",)
        categories = await repo.list()
        assert [c.name for c in categories] == ["A", "B", "C"]
        assert await repo.count(filters={"is_active": True}) == 2
        assert await repo.exists(name="C")
        found = await repo.get_by(name="A")
        assert found is not None and found.name == "A"

        # Protected fields ignored on update
        obj = await repo.update(created[0], {"id": 999, "name": "B2"})
        assert obj.id != 999 and obj.name == "B2"

        # Unknown fields rejected
        with pytest.raises(InvalidQueryError):
            await repo.update(obj, {"non_existent_column": 1})

        # Unknown filter operator rejected
        with pytest.raises(InvalidQueryError):
            await repo.list(filters={"name__unsupported_op": "x"})

        # Refusal to bulk-delete without filters
        with pytest.raises(InvalidQueryError):
            await repo.delete_where()

        # Delete with filter
        deleted_count = await repo.delete_where(is_active=False)
        assert deleted_count == 1
        assert await repo.count() == 2

    await engine.dispose()


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
        mock_categories.exists.return_value = False
        service._ensure_active_category = AsyncMock(
            side_effect=BusinessRuleError("Category id=999 does not exist or is inactive.")
        )
        await service.create(payload)

    service.repository.create.assert_not_awaited()
