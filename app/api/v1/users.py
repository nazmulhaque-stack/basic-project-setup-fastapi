"""Users: generated CRUD router using CRUDRouter with AES-256-GCM AEAD encryption and Redis Cluster caching."""

from typing import Annotated

from fastapi import Depends

from app.api.crud_router import CRUDRouter
from app.api.deps import provide
from app.models.user import User
from app.schemas.user import UserCreate, UserRead, UserUpdate
from app.services.user import UserService

# Cache-key prefix is explicit (not derived from the class name) so it never
# silently drifts: `cache:users:...` is the production Redis namespace.
get_user_service = provide(UserService, cache_prefix="users")
UserServiceDep = Annotated[UserService, Depends(get_user_service)]

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
    "UserServiceDep",
]
