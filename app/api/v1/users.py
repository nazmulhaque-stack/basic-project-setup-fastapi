"""User API Router (FastAPI Official Documentation Style).

Endpoints for User CRUD, keyset pagination, aggregated profile queries with Redis caching,
and full-text Elasticsearch fuzzy search.
"""

from typing import Annotated, List, Optional
import uuid

from fastapi import APIRouter, Body, Query, status

from app.schemas.outbox import OutboxEventRead
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


@router.get(
    "/outbox",
    response_model=List[OutboxEventRead],
    summary="List Outbox events",
    description="Returns Transactional Outbox records tracking PostgreSQL -> Elasticsearch synchronization status, publish timestamp, and retry count.",
)
async def list_outbox_events(
    service: UserServiceDep,
    limit: Annotated[int, Query(ge=1, le=100, description="Max outbox records")] = 50,
    status: Annotated[Optional[str], Query(description="Filter by outbox status: PENDING, PUBLISHED, FAILED")] = None,
) -> List[OutboxEventRead]:
    events = await service.list_outbox_events(limit=limit, status=status)
    return [
        OutboxEventRead(
            id=e.id,
            aggregate_type=e.aggregate_type,
            aggregate_id=e.aggregate_id,
            event_type=e.event_type,
            payload=e.payload,
            status=e.status,
            retry_count=e.retry_count,
            last_error=e.last_error,
            created_at=e.created_at,
            published_at=e.published_at,
        )
        for e in events
    ]


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
    payload: Annotated[
        UserCreate,
        Body(
            openapi_examples={
                "corporate_admin": {
                    "summary": "Administrator with Corporate Account",
                    "description": "Full-featured user in Engineering department with an initial corporate account.",
                    "value": {
                        "name": "Sarah Connor",
                        "email": "sarah.connor@example.com",
                        "role": "Administrator",
                        "department": "Engineering",
                        "phone": "+14155552671",
                        "accounts": [
                            {
                                "account_number": "ACC-CORP-1001",
                                "account_type": "corporate",
                                "balance_cents": 500000,
                                "currency": "USD",
                                "status": "active"
                            }
                        ]
                    }
                },
                "standard_user": {
                    "summary": "Standard User (Without Accounts)",
                    "description": "Basic user with Operations department and standard user role.",
                    "value": {
                        "name": "Alex Mercer",
                        "email": "alex.mercer@example.com",
                        "role": "user",
                        "department": "Operations",
                        "phone": "+14155559876"
                    }
                }
            }
        )
    ],
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
    description="Complex multi-table query joining User, Department, Role, and UserAccounts with Redis Cache-Aside, Elasticsearch CQRS indexing, and dynamic revalidation.",
)
async def get_user_profile(
    user_id: uuid.UUID,
    service: UserServiceDep,
    revalidate: Annotated[bool, Query(description="Force refresh from source of truth and update Redis/ES")] = False,
) -> UserProfileRead:
    return await service.get_user_profile(user_id, revalidate=revalidate)


@router.post(
    "/{user_id}/accounts",
    response_model=AccountRead,
    status_code=status.HTTP_201_CREATED,
    summary="Add account to user",
    description="Attaches a new account to the user, invalidates profile cache, and updates Elasticsearch.",
)
async def add_user_account(
    user_id: uuid.UUID,
    payload: Annotated[
        AccountCreate,
        Body(
            openapi_examples={
                "savings": {
                    "summary": "Savings Account",
                    "value": {
                        "account_number": "ACC-SAV-2026",
                        "account_type": "savings",
                        "balance_cents": 250000,
                        "currency": "USD",
                        "status": "active"
                    }
                },
                "corporate": {
                    "summary": "Corporate Checking Account",
                    "value": {
                        "account_number": "ACC-CORP-5050",
                        "account_type": "corporate",
                        "balance_cents": 1000000,
                        "currency": "USD",
                        "status": "active"
                    }
                }
            }
        )
    ],
    service: UserServiceDep,
) -> AccountRead:
    return await service.add_user_account(user_id, payload)


@router.get(
    "/{user_id}",
    response_model=UserRead,
    summary="Get user by ID",
    description="Retrieves a user by ID using Redis cache-aside (sub-millisecond read) with dynamic revalidation support.",
)
async def get_user(
    user_id: uuid.UUID,
    service: UserServiceDep,
    revalidate: Annotated[bool, Query(description="Force refresh from source of truth and update Redis/ES")] = False,
) -> UserRead:
    return await service.get_user_by_id(user_id, revalidate=revalidate)


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
                        "phone": "+14155558899"
                    }
                },
                "promote_role": {
                    "summary": "Promote Role to Manager",
                    "value": {
                        "role": "Manager"
                    }
                }
            }
        )
    ],
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
