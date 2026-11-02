"""Categories: 100% generated CRUD using CRUDRouter."""

from typing import Annotated

from fastapi import Depends

from app.api.crud_router import CRUDRouter
from app.api.deps import provide
from app.models.category import Category
from app.schemas.category import CategoryCreate, CategoryRead, CategoryUpdate
from app.services.category import CategoryService

# Cache-key prefix is explicit (not derived from the class name) so it never
# silently drifts: `cache:categories:...` is the production Redis namespace.
get_category_service = provide(CategoryService, cache_prefix="categories")
CategoryServiceDep = Annotated[CategoryService, Depends(get_category_service)]

router = CRUDRouter(
    model=Category,
    service=get_category_service,
    create_schema=CategoryCreate,
    update_schema=CategoryUpdate,
    read_schema=CategoryRead,
    prefix="/categories",
    tags=["Categories"],
    filter_fields={
        "name": ["eq", "icontains"],
        "is_active": ["eq"],
    },
    sort_fields=["id", "name", "created_at"],
)

__all__ = ["router", "get_category_service", "CategoryServiceDep"]
