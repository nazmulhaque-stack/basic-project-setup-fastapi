"""User API Router (FastAPI Official Documentation Style).

Endpoints for User CRUD, keyset pagination, aggregated profile queries with Redis caching,
and full-text Elasticsearch fuzzy search.
"""

from typing import Annotated, List, Optional
import uuid

from fastapi import APIRouter, Query, status

from app.schemas.user import (
    AccountCreate,
    AccountRead,
    DepartmentRead,
    RoleRead,
    UserCreate,
    UserPage,
    UserProfileRead,
    UserRead,
    UserUpdate,
)
from app.services.user import UserServiceDep

router = APIRouter(prefix="/users", tags=["Users"])


# ------------------------------------------------------------------------------
# 1. Departments & Roles Lookups (Static paths before parameterized {user_id})
# ------------------------------------------------------------------------------
@router.get(
    "/departments",
    response_model=List[DepartmentRead],
    summary="List departments",
    description="Returns all active departments for user organization.",
)
async def list_departments(
    service: UserServiceDep,
) -> List[DepartmentRead]:
    return await service.list_departments()


@router.get(
    "/roles",
    response_model=List[RoleRead],
    summary="List roles",
    description="Returns all available authorization roles and permission sets.",
)
async def list_roles(
    service: UserServiceDep,
) -> List[RoleRead]:
    return await service.list_roles()


# ------------------------------------------------------------------------------
# 2. Elasticsearch Fuzzy Searches
# ------------------------------------------------------------------------------
@router.get(
    "/profiles/search",
    response_model=List[UserProfileRead],
    summary="Search user profiles",
    description="Full-text fuzzy search across aggregated profiles (names, departments, accounts) via Elasticsearch.",
)
async def search_profiles(
    service: UserServiceDep,
    q: Annotated[str, Query(min_length=1, description="Search term for name, department, role, or account")] = "",
    limit: Annotated[int, Query(ge=1, le=100, description="Max results")] = 20,
    offset: Annotated[int, Query(ge=0, description="Offset")] = 0,
) -> List[UserProfileRead]:
    return await service.search_user_profiles(q=q, limit=limit, offset=offset)


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


@router.post(
    "/sync-es",
    summary="Sync all users to Elasticsearch",
    description="Synchronizes all users and aggregated profiles from PostgreSQL into Elasticsearch.",
)
async def sync_users_to_es(
    service: UserServiceDep,
) -> dict:
    return await service.sync_all_to_es()


# ------------------------------------------------------------------------------
# 3. User CRUD Operations
# ------------------------------------------------------------------------------
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
    "/{user_id}/profile",
    response_model=UserProfileRead,
    summary="Get user profile",
    description="Complex multi-table query joining User, Department, Role, and UserAccounts with Redis Cache-Aside and Elasticsearch CQRS indexing.",
)
async def get_user_profile(
    user_id: uuid.UUID,
    service: UserServiceDep,
) -> UserProfileRead:
    return await service.get_user_profile(user_id)


@router.post(
    "/{user_id}/accounts",
    response_model=AccountRead,
    status_code=status.HTTP_201_CREATED,
    summary="Add account to user",
    description="Attaches a new account to the user, invalidates profile cache, and updates Elasticsearch.",
)
async def add_user_account(
    user_id: uuid.UUID,
    payload: AccountCreate,
    service: UserServiceDep,
) -> AccountRead:
    return await service.add_user_account(user_id, payload)


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
