"""Users: generated CRUD router using CRUDRouter with AES-256-GCM AEAD encryption and Redis Cluster caching."""

from app.api.crud_router import CRUDRouter
from app.api.deps import get_user_service
from app.models.user import User
from app.schemas.user import UserCreate, UserRead, UserUpdate

router = CRUDRouter(
    model=User,
    service=get_user_service,
    create_schema=UserCreate,
    update_schema=UserUpdate,
    read_schema=UserRead,
    prefix="/users",
    tags=["Users"],
    filter_fields={
        "name": ["icontains"],
        "email": ["eq", "icontains"],
        "role": ["eq", "in"],
    },
    sort_fields=["id", "name", "email", "role", "created_at"],
)


__all__ = [
    "router",
    "get_user_service",
]
