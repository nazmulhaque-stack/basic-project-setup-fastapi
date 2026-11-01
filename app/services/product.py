"""Product Service implementing catalog management, uniqueness rules, and caching.

Leverages the Template Method Pattern provided by BaseService to eliminate duplicate CRUD boilerplate.
"""

import logging
from typing import Any, Dict, List, Optional, Union
import uuid

from sqlalchemy.ext.asyncio import AsyncSession

from app.core.elasticsearch import IElasticsearchService
from app.core.exceptions import BusinessRuleError, ConflictError, NotFoundError
from app.models.product import Product
from app.repositories.product import ProductRepository
from app.schemas.product import ProductCreate, ProductPage, ProductRead, ProductUpdate
from app.services.base import BaseService
from app.services.interface import ICacheService

logger = logging.getLogger(__name__)


class ProductService(BaseService[Product, ProductCreate, ProductUpdate, ProductRead, uuid.UUID]):
    """Product domain service demonstrating clean domain logic on top of BaseService."""

    def __init__(
        self,
        db: Optional[AsyncSession] = None,
        repo: Optional[ProductRepository] = None,
        cache: Optional[ICacheService] = None,
        elasticsearch: Optional[IElasticsearchService] = None,
        outbox_enabled: bool = False,
        outbox_topic: str = "domain.events",
    ) -> None:
        resolved_repo: ProductRepository
        if repo is not None:
            resolved_repo = repo
        elif db is not None:
            resolved_repo = ProductRepository(db)
        else:
            raise ValueError("Either db session or repo must be provided to ProductService")

        super().__init__(
            repo=resolved_repo,
            read_schema_cls=ProductRead,
            cache=cache,
            elasticsearch=elasticsearch,
            order_col="created_at",
            id_col="id",
            db=db,
            outbox_enabled=outbox_enabled,
            outbox_topic=outbox_topic,
        )
        self.repository: ProductRepository = resolved_repo

    repository_class = ProductRepository

    # --------------------------------------------------------------------------
    # Pluggable Domain Lifecycle Hooks (Template Method Pattern)
    # --------------------------------------------------------------------------

    async def _ensure_active_category(self, category_id: int) -> None:
        if self.db is not None:
            from app.repositories.category import CategoryRepository
            categories = CategoryRepository(self.db)
            if not await categories.exists(id=category_id, is_active=True):
                raise BusinessRuleError(f"Category id={category_id} does not exist or is inactive.")

    async def before_create(self, payload: ProductCreate) -> Product:
        """Enforces unique SKU and active category validation before creating entity."""
        existing = await self.repository.get_by_sku(payload.sku)
        if existing is not None:
            raise ConflictError(f"Product with SKU '{payload.sku}' already exists.")
        if getattr(payload, "category_id", None) is not None:
            await self._ensure_active_category(payload.category_id)
        return await super().before_create(payload)

    async def before_update(self, entity: Product, payload: ProductUpdate) -> None:
        """Enforces unique SKU and active category validation if changed during partial update."""
        if (
            payload.stock is not None
            and payload.stock < (entity.reserved_quantity or 0)
        ):
            raise BusinessRuleError(
                "On-hand stock cannot be reduced below the quantity already reserved."
            )
        if payload.sku is not None:
            existing = await self.repository.get_by_sku(payload.sku)
            if existing is not None and str(existing.id) != str(entity.id):
                raise ConflictError(f"Product with SKU '{payload.sku}' already exists.")
        if getattr(payload, "category_id", None) is not None and payload.category_id != entity.category_id:
            await self._ensure_active_category(payload.category_id)
        await super().before_update(entity, payload)

    # --------------------------------------------------------------------------
    # Custom Use Cases (Atomic Stock Adjustment, Low Stock, & Detail Queries)
    # --------------------------------------------------------------------------

    async def get_detail(self, product_id: Union[uuid.UUID, int, str]) -> Any:
        """Loads product along with its category relation."""
        from app.schemas.product import ProductDetail
        product = await self.repository.get_with_category(product_id)
        if product is None:
            raise NotFoundError(f"Product with id={product_id} not found.")
        return ProductDetail.model_validate(product)

    async def adjust_stock(self, product_id: Union[uuid.UUID, int, str], delta: int) -> ProductRead:
        """Atomically adjusts product stock inventory."""
        product = await self.repo.get(product_id)
        if product is None:
            raise NotFoundError(f"Product with id={product_id} not found.")

        success = await self.repository.change_stock_atomic(product_id, delta)
        if not success:
            raise BusinessRuleError(f"Not enough stock to remove {-delta} unit(s).")

        self._enqueue_outbox_event("stock_adjusted", product)
        if self.db is not None:
            await self.db.commit()
            await self.db.refresh(product)

        # Invalidate item and list caches polymorphically
        await self.cache.delete_item(product_id)
        await self.cache.invalidate_lists()

        return await self.to_read_schema(product)

    async def low_stock(self, threshold: int = 5, limit: int = 50) -> List[ProductRead]:
        """Queries active products with stock under the threshold."""
        entities = await self.repository.low_stock(threshold=threshold, limit=limit)
        return [await self.to_read_schema(e) for e in entities]

    # --------------------------------------------------------------------------
    # Keyset Pagination Query
    # --------------------------------------------------------------------------

    async def list_products(
        self,
        limit: int = 50,
        cursor: Optional[str] = None,
        category: Optional[str] = None,
        is_active: Optional[bool] = None,
        cache_enabled: Optional[bool] = None,
    ) -> ProductPage:
        """Lists products via keyset cursor pagination with optional category filtering."""
        filters: Dict[str, Any] = {}
        if category is not None:
            filters["category"] = category
        if is_active is not None:
            filters["is_active"] = is_active

        page_dict: Dict[str, Any] = await self.list_keyset(
            limit=limit,
            cursor=cursor,
            cache_enabled=cache_enabled,
            **filters,
        )
        return ProductPage(**page_dict)

    # --------------------------------------------------------------------------
    # Backward-Compatible Method Aliases Directly Bound from BaseService
    # --------------------------------------------------------------------------
    create_product = BaseService.create
    get_product_by_id = BaseService.get_by_id
    update_product = BaseService.update_by_id
    delete_product = BaseService.delete_by_id


__all__ = ["ProductService"]
