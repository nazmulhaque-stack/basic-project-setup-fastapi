"""Product row dataclass."""

from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Optional
import uuid

from app.models.base import Base

if TYPE_CHECKING:
    from app.models.category import Category


@dataclass
class Product(Base):
    id: uuid.UUID
    name: str
    sku: str
    price_cents: int
    category: str = "general"
    category_id: Optional[int] = None
    is_active: bool = True
    stock: int = 0
    reserved_quantity: int = 0
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    # Populated only by ProductRepository.get_with_category() (the
    # hand-written equivalent of the old `selectinload(Product.category_rel)`).
    category_rel: "Optional[Category]" = None

    @property
    def available_quantity(self) -> int:
        """Quantity available to promise while preserving physical on-hand stock."""
        return (self.stock or 0) - (self.reserved_quantity or 0)

    def __repr__(self) -> str:
        return f"<Product id={self.id} sku={self.sku} name={self.name}>"


__all__ = ["Product"]
