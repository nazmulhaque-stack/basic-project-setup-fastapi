from app.models import Category
from app.repositories.base import SQLAlchemyRepository


class CategoryRepository(SQLAlchemyRepository[Category]):
    """Full CRUD is inherited. `model` is inferred from SQLAlchemyRepository[Category]."""

    default_sort = ("name",)
