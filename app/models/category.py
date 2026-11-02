from typing import List, Optional, TYPE_CHECKING

from sqlalchemy import Boolean, Index, String, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base, IdIntMixin, TimestampMixin

if TYPE_CHECKING:
    from app.models.product import Product


class Category(IdIntMixin, TimestampMixin, Base):
    """Category domain entity using integer primary key and timestamp mixins."""

    __tablename__ = "categories"

    # Critical composite index for O(log N) keyset pagination (WHERE (created_at, id) > ...).
    # CategoryService.list_keyset() seeks on (created_at, id) like every other entity; without
    # this, every category list page forces a full sort instead of an index seek.
    __table_args__ = (
        Index("ix_categories_created_at_id", "created_at", "id"),
    )

    name: Mapped[str] = mapped_column(String(100), unique=True, index=True, nullable=False)
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True, default=None)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)

    # lazy="raise": prevents accidental N+1 queries. Load explicitly with selectinload().
    products: Mapped[List["Product"]] = relationship(
        "Product", back_populates="category_rel", lazy="raise"
    )

    def __repr__(self) -> str:
        return f"<Category id={self.id} name={self.name}>"


__all__ = ["Category"]
