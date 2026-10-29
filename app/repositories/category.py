"""Category repository inheriting complete generic CRUD from SQLAlchemyRepository."""

from app.models.category import Category
from app.repositories.base import SQLAlchemyRepository


class CategoryRepository(SQLAlchemyRepository[Category]):
    """Full CRUD is inherited. `model` is inferred from SQLAlchemyRepository[Category]."""

    default_sort = ("name",)


__all__ = ["CategoryRepository"]
