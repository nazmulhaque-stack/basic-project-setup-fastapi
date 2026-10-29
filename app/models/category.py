from typing import TYPE_CHECKING

from sqlalchemy import String, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.models.base import Base, IdMixin, TimestampMixin

if TYPE_CHECKING:
    from app.models.product import Product


class Category(IdMixin, TimestampMixin, Base):
    __tablename__ = "categories"

    name: Mapped[str] = mapped_column(String(100), unique=True, index=True)
    description: Mapped[str | None] = mapped_column(Text, default=None)
    is_active: Mapped[bool] = mapped_column(default=True)

    # lazy="raise": accidental lazy loads become a clear error instead of a
    # confusing MissingGreenlet in async code. Load explicitly with selectinload().
    products: Mapped[list["Product"]] = relationship(
        back_populates="category", lazy="raise"
    )
