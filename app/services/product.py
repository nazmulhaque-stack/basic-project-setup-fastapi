from collections.abc import Sequence
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from app.core.exceptions import BusinessRuleError, NotFoundError
from app.models import Product
from app.repositories import CategoryRepository, ProductRepository
from app.schemas import ProductCreate, ProductUpdate
from app.services.base import BaseService


class ProductService(BaseService[Product, ProductCreate, ProductUpdate]):
    repository_class = ProductRepository
    repository: ProductRepository  # narrow the type for the custom methods below

    def __init__(self, session: AsyncSession) -> None:
        super().__init__(session)
        self.categories = CategoryRepository(session)  # same session, same transaction

    # ---- business rules via hooks (create/update/delete logic is inherited) ----
    async def _ensure_active_category(self, category_id: int) -> None:
        if not await self.categories.exists(id=category_id, is_active=True):
            raise BusinessRuleError(f"Category id={category_id} does not exist or is inactive.")

    async def before_create(self, data: dict[str, Any]) -> dict[str, Any]:
        await self._ensure_active_category(data["category_id"])
        return data

    async def before_update(self, obj: Product, data: dict[str, Any]) -> dict[str, Any]:
        if "category_id" in data and data["category_id"] != obj.category_id:
            await self._ensure_active_category(data["category_id"])
        return data

    # ---- use cases that the generic CRUD does not cover ----
    async def get_detail(self, product_id: int) -> Product:
        product = await self.repository.get_with_category(product_id)
        if product is None:
            raise NotFoundError(f"Product with id={product_id} not found.")
        return product

    async def low_stock(self, threshold: int) -> Sequence[Product]:
        return await self.repository.low_stock(threshold)

    async def adjust_stock(self, product_id: int, delta: int) -> Product:
        product = await self.get(product_id)  # 404 if missing
        if not await self.repository.change_stock_atomic(product_id, delta):
            raise BusinessRuleError(f"Not enough stock to remove {-delta} unit(s).")
        await self.session.commit()
        await self.session.refresh(product)  # load the new stock / updated_at
        return product
