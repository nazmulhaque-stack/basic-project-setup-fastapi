from decimal import Decimal
from typing import TYPE_CHECKING

from sqlalchemy import ForeignKey, Numeric, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.models.base import Base, IdMixin, TimestampMixin

if TYPE_CHECKING:
    from app.models.category import Category


class Product(IdMixin, TimestampMixin, Base):
    __tablename__ = "products"

    name: Mapped[str] = mapped_column(String(150), index=True)
    sku: Mapped[str] = mapped_column(String(64), unique=True)
    price: Mapped[Decimal] = mapped_column(Numeric(12, 2))  # never float for money
    stock: Mapped[int] = mapped_column(default=0)
    is_active: Mapped[bool] = mapped_column(default=True)

    # RESTRICT: a category that still has products cannot be deleted (-> 409).
    category_id: Mapped[int] = mapped_column(
        ForeignKey("categories.id", ondelete="RESTRICT"), index=True
    )
    category: Mapped["Category"] = relationship(back_populates="products", lazy="raise")
