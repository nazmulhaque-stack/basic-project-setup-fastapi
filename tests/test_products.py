"""Unit tests for Product entity, ProductRepository, and ProductService."""

from datetime import datetime, timezone
from typing import Any
from unittest.mock import AsyncMock, MagicMock
import uuid
import pytest

from app.core.exceptions import NotFoundError
from app.models.product import Product
from app.repositories.product import ProductRepository
from app.schemas.product import ProductCreate, ProductPage, ProductRead
from app.services.product import ProductService
from app.services.cache import DynamicRedisCache


@pytest.fixture
def mock_product_repo() -> ProductRepository:
    repo: MagicMock = MagicMock(spec=ProductRepository)
    repo.model = Product
    repo.session = AsyncMock()
    repo.db = repo.session
    repo.get_by_sku = AsyncMock(return_value=None)
    return repo


@pytest.mark.asyncio
async def test_product_service_create_success(
    mock_product_repo: ProductRepository,
    mock_redis: Any,
) -> None:
    """Verifies product creation delegates to repo and returns ProductRead."""
    cache: DynamicRedisCache = DynamicRedisCache(redis=mock_redis, prefix="products")
    service: ProductService = ProductService(repo=mock_product_repo, cache=cache)

    mock_product_repo.create = AsyncMock()

    payload: ProductCreate = ProductCreate(
        name="Mechanical Keyboard",
        sku="KB-MECH-01",
        price_cents=12999,
        category="electronics",
        is_active=True,
    )

    result: ProductRead = await service.create_product(payload)

    assert mock_product_repo.create.call_count == 1
    call_entity: Product = mock_product_repo.create.call_args[0][0]
    assert isinstance(call_entity, Product)
    assert call_entity.name == "Mechanical Keyboard"
    assert call_entity.sku == "KB-MECH-01"
    assert call_entity.price_cents == 12999
    assert result.name == "Mechanical Keyboard"


@pytest.mark.asyncio
async def test_product_service_get_by_id_not_found(
    mock_product_repo: ProductRepository,
    mock_redis: Any,
) -> None:
    """Verifies that requesting a non-existent product ID raises NotFoundError."""
    cache: DynamicRedisCache = DynamicRedisCache(redis=mock_redis, prefix="products")
    service: ProductService = ProductService(repo=mock_product_repo, cache=cache)

    product_id: uuid.UUID = uuid.uuid4()
    mock_product_repo.get = AsyncMock(return_value=None)

    with pytest.raises(NotFoundError):
        await service.get_product_by_id(product_id)


@pytest.mark.asyncio
async def test_product_service_list_products_keyset(
    mock_product_repo: ProductRepository,
    mock_redis: Any,
) -> None:
    """Verifies keyset pagination for product catalog."""
    cache: DynamicRedisCache = DynamicRedisCache(redis=mock_redis, prefix="products")
    service: ProductService = ProductService(repo=mock_product_repo, cache=cache)

    p1: Product = Product(
        id=uuid.uuid4(),
        name="Mouse",
        sku="MS-01",
        price_cents=4999,
        category="electronics",
        is_active=True,
        created_at=datetime.now(timezone.utc),
    )

    mock_product_repo.list_keyset = AsyncMock(return_value=([p1], False))

    page: ProductPage = await service.list_products(limit=10, category="electronics")

    assert len(page.items) == 1
    assert page.items[0].sku == "MS-01"
    assert page.next_cursor is None


@pytest.mark.asyncio
async def test_products_custom_endpoints() -> None:
    """Verifies product custom endpoints (/low-stock, /{item_id}/detail, /{item_id}/stock)."""
    from app.api.v1.products import adjust_stock, low_stock, product_detail
    from app.schemas.product import ProductDetail, StockAdjust

    mock_service = MagicMock()
    product_id = uuid.uuid4()
    dummy_read = ProductRead(
        id=product_id,
        name="Endpoint Product",
        sku="EP-001",
        price_cents=2999,
        category="electronics",
        is_active=True,
        created_at=datetime.now(timezone.utc),
    )

    # 1. low_stock
    mock_service.low_stock = AsyncMock(return_value=[dummy_read])
    res_low = await low_stock(service=mock_service, threshold=5)
    assert len(res_low) == 1
    assert res_low[0].sku == "EP-001"

    # 2. product_detail
    detail = ProductDetail(
        id=product_id,
        name="Endpoint Product",
        sku="EP-001",
        price_cents=2999,
        category="electronics",
        is_active=True,
        created_at=datetime.now(timezone.utc),
    )
    mock_service.get_detail = AsyncMock(return_value=detail)
    res_detail = await product_detail(item_id=product_id, service=mock_service)
    assert res_detail.id == product_id

    # 3. adjust_stock
    mock_service.adjust_stock = AsyncMock(return_value=dummy_read)
    res_stock = await adjust_stock(item_id=product_id, payload=StockAdjust(delta=10), service=mock_service)
    assert res_stock.id == product_id

