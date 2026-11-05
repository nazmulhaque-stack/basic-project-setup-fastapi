"""User API Router (FastAPI Official Documentation Style).

Endpoints for User CRUD, keyset pagination, and full-text Elasticsearch fuzzy search.
"""

from typing import Annotated, List, Optional
import uuid

from fastapi import APIRouter, Query, status

from app.schemas.user import UserCreate, UserPage, UserRead, UserUpdate
from app.services.user import UserServiceDep

router = APIRouter(prefix="/users", tags=["Users"])


@router.post(
    "",
    response_model=UserRead,
    status_code=status.HTTP_201_CREATED,
    summary="Create user",
    description="Creates a user with AES-256-GCM AEAD encryption, HMAC blind indexing, Redis caching, and direct Elasticsearch indexing.",
)
async def create_user(
    payload: UserCreate,
    service: UserServiceDep,
) -> UserRead:
    return await service.create_user(payload)


@router.get(
    "",
    response_model=UserPage,
    summary="List users",
    description="Returns keyset (cursor) paginated users with Redis caching.",
)
async def list_users(
    service: UserServiceDep,
    limit: Annotated[int, Query(ge=1, le=100, description="Page limit")] = 50,
    cursor: Annotated[Optional[str], Query(description="Keyset pagination cursor token")] = None,
    role: Annotated[Optional[str], Query(description="Filter by user role")] = None,
) -> UserPage:
    return await service.list_users(limit=limit, cursor=cursor, role=role)


@router.get(
    "/search",
    response_model=List[UserRead],
    summary="Search users",
    description="Full-text fuzzy search across users powered directly by Elasticsearch.",
)
async def search_users(
    service: UserServiceDep,
    q: Annotated[str, Query(min_length=1, description="Search query string")] = "",
    limit: Annotated[int, Query(ge=1, le=100, description="Max search results")] = 20,
    offset: Annotated[int, Query(ge=0, description="Search result offset")] = 0,
) -> List[UserRead]:
    return await service.search_users(q=q, limit=limit, offset=offset)


@router.get(
    "/{user_id}",
    response_model=UserRead,
    summary="Get user by ID",
    description="Retrieves a user by ID using Redis cache-aside (sub-millisecond read).",
)
async def get_user(
    user_id: uuid.UUID,
    service: UserServiceDep,
) -> UserRead:
    return await service.get_user_by_id(user_id)


@router.patch(
    "/{user_id}",
    response_model=UserRead,
    summary="Update user",
    description="Partially updates a user, re-encrypts phone if modified, invalidates Redis cache, and updates Elasticsearch.",
)
async def update_user(
    user_id: uuid.UUID,
    payload: UserUpdate,
    service: UserServiceDep,
) -> UserRead:
    return await service.update_user(user_id, payload)


@router.delete(
    "/{user_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Delete user",
    description="Deletes a user from PostgreSQL, purges Redis cache, and removes the document from Elasticsearch.",
)
async def delete_user(
    user_id: uuid.UUID,
    service: UserServiceDep,
) -> None:
    await service.delete_user(user_id)


__all__ = ["router"]
