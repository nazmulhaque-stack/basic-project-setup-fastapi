from collections.abc import Sequence

from sqlalchemy import select, update
from sqlalchemy.orm import selectinload

from app.models import Product
from app.repositories.base import SQLAlchemyRepository


class ProductRepository(SQLAlchemyRepository[Product]):
    """Generic CRUD inherited + model-specific queries added below."""

    async def get_with_category(self, product_id: int) -> Product | None:
        # Explicit eager load: relationships are lazy="raise", so this is the only way in.
        return await self.get(product_id, options=[selectinload(Product.category)])

    async def change_stock_atomic(self, product_id: int, delta: int) -> bool:
        """UPDATE ... SET stock = stock + :delta WHERE stock + :delta >= 0.

        One atomic statement: two concurrent requests can never oversell, unlike
        read -> modify in Python -> write. Returns False if the guard blocked it.
        """
        stmt = (
            update(Product)
            .where(Product.id == product_id, Product.stock + delta >= 0)
            .values(stock=Product.stock + delta)
            .execution_options(synchronize_session=False)
        )
        result = await self.session.execute(stmt)
        return bool(result.rowcount == 1)  # type: ignore[attr-defined]

    async def low_stock(self, threshold: int, limit: int = 50) -> Sequence[Product]:
        stmt = (
            select(Product)
            .where(Product.is_active.is_(True), Product.stock <= threshold)
            .order_by(Product.stock.asc(), Product.id.asc())
            .limit(limit)
        )
        return (await self.session.scalars(stmt)).all()
