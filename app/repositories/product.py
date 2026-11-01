from collections.abc import Sequence
from typing import Optional, Union
import uuid
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.product import Product
from app.repositories.base import BaseRepository


class ProductRepository(BaseRepository[Product, uuid.UUID]):
    """Concrete repository for Product domain entity extending BaseRepository."""

    def __init__(self, db: AsyncSession) -> None:
        super().__init__(model=Product, db=db)

    async def get_by_sku(self, sku: str) -> Optional[Product]:
        """Finds product by unique SKU."""
        stmt = select(Product).where(Product.sku == sku)
        return await self.db.scalar(stmt)

    async def change_stock_atomic(self, product_id: Union[uuid.UUID, int, str], delta: int) -> bool:
        """Atomically updates inventory stock without race conditions:
        UPDATE products SET stock = stock + delta WHERE id = :id AND stock + delta >= 0.
        """
        stmt = (
            update(Product)
            .where(
                Product.id == product_id,
                Product.stock + delta >= Product.reserved_quantity,
            )
            .values(stock=Product.stock + delta)
            .execution_options(synchronize_session=False)
        )
        result = await self.db.execute(stmt)
        return bool(getattr(result, "rowcount", 0) == 1)

    async def low_stock(self, threshold: int = 5, limit: int = 50) -> Sequence[Product]:
        """Fetches active products with inventory stock under or equal to threshold."""
        stmt = (
            select(Product)
            .where(
                Product.is_active.is_(True),
                Product.stock - Product.reserved_quantity <= threshold,
            )
            .order_by(
                (Product.stock - Product.reserved_quantity).asc(),
                Product.id.asc(),
            )
            .limit(limit)
        )
        return (await self.db.scalars(stmt)).all()

    async def get_with_category(self, product_id: Union[uuid.UUID, int, str]) -> Optional[Product]:
        """Eagerly loads product with its associated category using selectinload (since relations are lazy='raise')."""
        from sqlalchemy.orm import selectinload
        return await self.get(product_id, options=[selectinload(Product.category_rel)])


__all__ = ["ProductRepository"]
