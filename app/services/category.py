from app.models import Category
from app.repositories import CategoryRepository
from app.schemas import CategoryCreate, CategoryUpdate
from app.services.base import BaseService


class CategoryService(BaseService[Category, CategoryCreate, CategoryUpdate]):
    """No custom rules: everything is inherited from BaseService."""

    repository_class = CategoryRepository
