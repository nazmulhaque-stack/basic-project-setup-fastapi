"""Products: generated CRUD + custom endpoints added to the same router."""

from typing import Annotated

from fastapi import Body, Query

from app.api.crud_router import CRUDRouter
from app.api.deps import ProductServiceDep, get_product_service
from app.models import Product
from app.schemas import ProductCreate, ProductDetail, ProductRead, ProductUpdate
from app.schemas.base import InputSchema

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
        "price": ["gte", "lte"],
        "stock": ["lte"],
        "category_id": ["eq", "in"],
        "is_active": ["eq"],
    },
    sort_fields=["id", "name", "price", "stock", "created_at"],
    # Example: protect writes later with auth, e.g. {"delete": [Depends(require_admin)]}
)


# ---- custom endpoints: the generic routes use /{item_id:int}, so no path clashes ----
@router.get("/low-stock", response_model=list[ProductRead], summary="Active products low on stock")
async def low_stock(
    service: ProductServiceDep,
    threshold: Annotated[int, Query(ge=0, le=10_000)] = 5,
):
    return await service.low_stock(threshold)


@router.get("/{item_id:int}/detail", response_model=ProductDetail, summary="Product with its category")
async def product_detail(item_id: int, service: ProductServiceDep):
    return await service.get_detail(item_id)


class StockAdjust(InputSchema):
    delta: int  # +10 to restock, -2 to sell


@router.post("/{item_id:int}/stock", response_model=ProductRead, summary="Atomically adjust stock")
async def adjust_stock(
    item_id: int, payload: Annotated[StockAdjust, Body()], service: ProductServiceDep
):
    return await service.adjust_stock(item_id, payload.delta)
