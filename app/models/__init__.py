"""Import every model here so Base.metadata knows all tables (Alembic, create_all)."""

from app.models.base import Base
from app.models.category import Category
from app.models.product import Product

__all__ = ["Base", "Category", "Product"]
