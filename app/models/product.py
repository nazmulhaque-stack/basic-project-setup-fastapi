"""SQLAlchemy 2.0 mapped Product entity.

Demonstrates how standard domain models leverage BaseRepository and BaseService
without needing custom CRUD code.
"""

from datetime import datetime
from typing import Optional, TYPE_CHECKING
import uuid

from sqlalchemy import (
    Boolean,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    Uuid,
    func,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.models.base import Base

if TYPE_CHECKING:
    from app.models.category import Category


class Product(Base):
    """Product domain entity."""

    __tablename__ = "products"

    id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
        nullable=False,
    )

    name: Mapped[str] = mapped_column(
        String(255),
        nullable=False,
    )

    sku: Mapped[str] = mapped_column(
        String(100),
        unique=True,
        index=True,
        nullable=False,
    )

    price_cents: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
    )

    category: Mapped[str] = mapped_column(
        String(100),
        nullable=False,
        default="general",
        index=True,
    )

    # Foreign key to categories table with RESTRICT on delete (-> 409 Conflict)
    category_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("categories.id", ondelete="RESTRICT"),
        nullable=True,
        index=True,
    )
    category_rel: Mapped[Optional["Category"]] = relationship(
        "Category",
        back_populates="products",
        lazy="raise",
    )

    is_active: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=True,
    )

    stock: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=0,
    )

    reserved_quantity: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=0,
        server_default="0",
    )

    @property
    def available_quantity(self) -> int:
        """Quantity available to promise while preserving physical on-hand stock."""
        return (self.stock or 0) - (self.reserved_quantity or 0)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )

    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )

    __table_args__ = (
        # Composite index for O(log N) keyset pagination
        Index("ix_products_created_at_id", "created_at", "id"),
        CheckConstraint(
            "reserved_quantity >= 0 AND reserved_quantity <= stock",
            name="reserved_quantity_lte_stock",
        ),
    )

    def __repr__(self) -> str:
        return f"<Product id={self.id} sku={self.sku} name={self.name}>"
