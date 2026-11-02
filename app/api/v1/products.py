"""Products: generated CRUD router + custom endpoints added to the same router."""

from typing import Annotated, Union
import uuid

from fastapi import Body, Depends, Query

from app.api.crud_router import CRUDRouter
from app.api.deps import provide
from app.models.product import Product
from app.schemas.product import (
    ProductCreate,
    ProductDetail,
    ProductRead,
    ProductUpdate,
    StockAdjust,
)
from app.services.product import ProductService

# Cache-key prefix is explicit (not derived from the class name) so it never
# silently drifts: `cache:products:...` is the production Redis namespace.
get_product_service = provide(ProductService, cache_prefix="products")
ProductServiceDep = Annotated[ProductService, Depends(get_product_service)]

router = CRUDRouter(
    model=Product,
    service=get_product_service,
    create_schema=ProductCreate,
    update_schema=ProductUpdate,
    read_schema=ProductRead,
    prefix="/products",
    tags=["Products"],
    filter_fields={
        "name": ["icontains"],
        "sku": ["eq", "startswith"],
        "category": ["eq", "in"],
        "category_id": ["eq", "in"],
        "is_active": ["eq"],
        "stock": ["lte"],
    },
    sort_fields=["id", "name", "price_cents", "stock", "created_at"],
)


# ---- Custom endpoints: the generic routes use /{item_id}, so no path clashes ----
@router.get("/low-stock", response_model=list[ProductRead], summary="Active products low on stock")
async def low_stock(
    service: ProductServiceDep,
    threshold: Annotated[int, Query(ge=0, le=10_000)] = 5,
):
    return await service.low_stock(threshold)


@router.get("/{item_id}/detail", response_model=ProductDetail, summary="Product with its category")
async def product_detail(item_id: Union[uuid.UUID, int], service: ProductServiceDep):
    return await service.get_detail(item_id)


@router.post("/{item_id}/stock", response_model=ProductRead, summary="Atomically adjust stock")
async def adjust_stock(
    item_id: Union[uuid.UUID, int],
    payload: Annotated[StockAdjust, Body()],
    service: ProductServiceDep,
):
    return await service.adjust_stock(item_id, payload.delta)


__all__ = [
    "router",
    "get_product_service",
    "ProductServiceDep",
    "low_stock",
    "product_detail",
    "adjust_stock",
]
