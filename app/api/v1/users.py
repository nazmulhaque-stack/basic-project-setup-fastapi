"""User API Router (FastAPI Official Documentation Style).

Endpoints for User 5 CRUD operations and full-text Elasticsearch fuzzy search:
1. POST /users           -> Create user
2. GET /users            -> List users (Keyset B-Tree pagination with Redis caching)
3. GET /users/search     -> Search users (Elasticsearch fuzzy full-text search)
4. GET /users/{user_id}  -> Get user by ID (Redis L1 -> ES L2 -> PostgreSQL L3 self-healing)
5. PATCH /users/{user_id}-> Update user (Selective re-encryption & cache purge)
6. DELETE /users/{user_id}-> Delete user (Purge cache & Transactional Outbox)
"""

from typing import Annotated, List, Optional
import uuid

from fastapi import APIRouter, Body, Query, status

from app.schemas.user import (
    UserCreate,
    UserPage,
    UserRead,
    UserUpdate,
)
from app.services.user import UserServiceDep

router = APIRouter(prefix="/users", tags=["Users"])


# ------------------------------------------------------------------------------
# 1. Full-Text Search (Static path declared BEFORE parameterized {user_id})
# ------------------------------------------------------------------------------
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


# ------------------------------------------------------------------------------
# 2. CREATE USER (POST /users)
# ------------------------------------------------------------------------------
@router.post(
    "",
    response_model=UserRead,
    status_code=status.HTTP_201_CREATED,
    summary="Create user",
    description="Creates a user with AES-256-GCM AEAD encryption, HMAC blind indexing, Redis caching, and Elasticsearch sync.",
)
async def create_user(
    payload: Annotated[
        UserCreate,
        Body(
            openapi_examples={
                "standard_user": {
                    "summary": "Standard User",
                    "description": "User with department and role.",
                    "value": {
                        "name": "Sarah Connor",
                        "email": "sarah.connor@example.com",
                        "role": "Administrator",
                        "department": "Engineering",
                        "phone": "+14155552671",
                    },
                }
            }
        ),
    ],
    service: UserServiceDep,
) -> UserRead:
    return await service.create_user(payload)


# ------------------------------------------------------------------------------
# 3. LIST USERS (GET /users)
# ------------------------------------------------------------------------------
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


# ------------------------------------------------------------------------------
# 4. GET USER BY ID (GET /users/{user_id})
# ------------------------------------------------------------------------------
@router.get(
    "/{user_id}",
    response_model=UserRead,
    summary="Get user by ID",
    description="Retrieves a user by ID using 3-tier self-healing fallback (Redis -> ES -> PostgreSQL).",
)
async def get_user(
    user_id: uuid.UUID,
    service: UserServiceDep,
) -> UserRead:
    return await service.get_user_by_id(user_id)


# ------------------------------------------------------------------------------
# 5. UPDATE USER (PATCH /users/{user_id})
# ------------------------------------------------------------------------------
@router.patch(
    "/{user_id}",
    response_model=UserRead,
    summary="Update user",
    description="Partially updates a user, re-encrypts phone if modified, invalidates Redis cache, and updates Elasticsearch.",
)
async def update_user(
    user_id: uuid.UUID,
    payload: Annotated[
        UserUpdate,
        Body(
            openapi_examples={
                "update_department_and_phone": {
                    "summary": "Update Department & Phone",
                    "value": {
                        "department": "Finance",
                        "phone": "+14155558899",
                    },
                },
                "promote_role": {
                    "summary": "Promote Role to Manager",
                    "value": {
                        "role": "Manager",
                    },
                },
            }
        ),
    ],
    service: UserServiceDep,
) -> UserRead:
    return await service.update_user(user_id, payload)


# ------------------------------------------------------------------------------
# 6. DELETE USER (DELETE /users/{user_id})
# ------------------------------------------------------------------------------
@router.delete(
    "/{user_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Delete user",
    description="Deletes a user from PostgreSQL, purges Redis cache, and records delete event in Transactional Outbox.",
)
async def delete_user(
    user_id: uuid.UUID,
    service: UserServiceDep,
) -> None:
    await service.delete_user(user_id)


__all__ = ["router"]
