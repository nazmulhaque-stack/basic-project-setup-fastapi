"""Categories: 100% generated CRUD using CRUDRouter."""

from app.api.crud_router import CRUDRouter
from app.api.deps import get_category_service
from app.models.category import Category
from app.schemas.category import CategoryCreate, CategoryRead, CategoryUpdate

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

__all__ = ["router", "get_category_service"]
