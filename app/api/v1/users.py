"""User API Router (FastAPI Official Documentation Style).

Endpoints for User 5 CRUD operations and full-text Elasticsearch fuzzy search:
1. POST /users           -> Create user
2. GET /users            -> List users (Keyset B-Tree pagination with Redis caching)
3. GET /users/search     -> Search users (Elasticsearch fuzzy full-text search)
4. GET /users/{user_id}  -> Get user by ID (Redis L1 -> ES L2 -> PostgreSQL L3 self-healing)
5. PATCH /users/{user_id}-> Update user (Selective re-encryption & cache purge)
6. DELETE /users/{user_id}-> Delete user (Purge cache & Transactional Outbox)
"""

from datetime import datetime
from typing import Annotated, Any, Dict, List, Optional
import uuid

from fastapi import APIRouter, Body, Query, status

from app.schemas.user import (
    UserCreate,
    UserPage,
    UserRead,
    UserSearchQuery,
    UserSearchResult,
    UserUpdate,
    UserProfileRead,
)
from app.services.user import UserServiceDep

router = APIRouter(prefix="/users", tags=["Users"])


# ------------------------------------------------------------------------------
# 1. Complex Search (Static paths declared BEFORE parameterized {user_id})
# ------------------------------------------------------------------------------
@router.get(
    "/search",
    response_model=UserSearchResult,
    summary="Search users with complex filters",
    description="Multi-field full-text fuzzy search with faceted filters, numeric balance/account ranges, date ranges, sorting, and optional aggregations.",
)
async def search_users(
    service: UserServiceDep,
    q: Annotated[Optional[str], Query(description="Full-text fuzzy search query matching name, email, department, role, account numbers")] = None,
    department: Annotated[Optional[str], Query(description="Filter by department name or code")] = None,
    role: Annotated[Optional[str], Query(description="Filter by authorization role")] = None,
    account_type: Annotated[Optional[str], Query(description="Filter by account type: corporate, savings, checking, investment")] = None,
    currency: Annotated[Optional[str], Query(description="Filter by 3-letter currency code (e.g. USD, EUR, GBP)")] = None,
    min_balance_cents: Annotated[Optional[int], Query(ge=0, description="Minimum total balance across accounts in cents")] = None,
    max_balance_cents: Annotated[Optional[int], Query(ge=0, description="Maximum total balance across accounts in cents")] = None,
    min_accounts: Annotated[Optional[int], Query(ge=0, description="Minimum number of attached accounts")] = None,
    max_accounts: Annotated[Optional[int], Query(ge=0, description="Maximum number of attached accounts")] = None,
    created_after: Annotated[Optional[datetime], Query(description="Filter users created on or after this timestamp")] = None,
    created_before: Annotated[Optional[datetime], Query(description="Filter users created on or before this timestamp")] = None,
    sort_by: Annotated[Optional[str], Query(description="Sort field: created_at, total_balance_cents, account_count, name, email, _score")] = "created_at",
    sort_order: Annotated[Optional[str], Query(pattern="^(asc|desc|ASC|DESC)$", description="Sort direction: asc or desc")] = "desc",
    include_aggregations: Annotated[bool, Query(description="Compute and return faceted aggregations by department, role, currency, and balance stats")] = False,
    limit: Annotated[int, Query(ge=1, le=100, description="Max results per page")] = 20,
    offset: Annotated[int, Query(ge=0, description="Search result offset")] = 0,
) -> UserSearchResult:
    query = UserSearchQuery(
        q=q,
        department=department,
        role=role,
        account_type=account_type,
        currency=currency,
        min_balance_cents=min_balance_cents,
        max_balance_cents=max_balance_cents,
        min_accounts=min_accounts,
        max_accounts=max_accounts,
        created_after=created_after,
        created_before=created_before,
        sort_by=sort_by or "created_at",
        sort_order=sort_order or "desc",
        include_aggregations=include_aggregations,
        limit=limit,
        offset=offset,
    )
    return await service.search_users(query)


@router.get(
    "/search-no-cache",
    response_model=UserSearchResult,
    summary="Search users with complex filters",
    description="Multi-field full-text fuzzy search with faceted filters, numeric balance/account ranges, date ranges, sorting, and optional aggregations.",
)
async def search_users(
    service: UserServiceDep,
    q: Annotated[Optional[str], Query(description="Full-text fuzzy search query matching name, email, department, role, account numbers")] = None,
    department: Annotated[Optional[str], Query(description="Filter by department name or code")] = None,
    role: Annotated[Optional[str], Query(description="Filter by authorization role")] = None,
    account_type: Annotated[Optional[str], Query(description="Filter by account type: corporate, savings, checking, investment")] = None,
    currency: Annotated[Optional[str], Query(description="Filter by 3-letter currency code (e.g. USD, EUR, GBP)")] = None,
    min_balance_cents: Annotated[Optional[int], Query(ge=0, description="Minimum total balance across accounts in cents")] = None,
    max_balance_cents: Annotated[Optional[int], Query(ge=0, description="Maximum total balance across accounts in cents")] = None,
    min_accounts: Annotated[Optional[int], Query(ge=0, description="Minimum number of attached accounts")] = None,
    max_accounts: Annotated[Optional[int], Query(ge=0, description="Maximum number of attached accounts")] = None,
    created_after: Annotated[Optional[datetime], Query(description="Filter users created on or after this timestamp")] = None,
    created_before: Annotated[Optional[datetime], Query(description="Filter users created on or before this timestamp")] = None,
    sort_by: Annotated[Optional[str], Query(description="Sort field: created_at, total_balance_cents, account_count, name, email, _score")] = "created_at",
    sort_order: Annotated[Optional[str], Query(pattern="^(asc|desc|ASC|DESC)$", description="Sort direction: asc or desc")] = "desc",
    include_aggregations: Annotated[bool, Query(description="Compute and return faceted aggregations by department, role, currency, and balance stats")] = False,
    limit: Annotated[int, Query(ge=1, le=100, description="Max results per page")] = 20,
    offset: Annotated[int, Query(ge=0, description="Search result offset")] = 0,
) -> UserSearchResult:
    query = UserSearchQuery(
        q=q,
        department=department,
        role=role,
        account_type=account_type,
        currency=currency,
        min_balance_cents=min_balance_cents,
        max_balance_cents=max_balance_cents,
        min_accounts=min_accounts,
        max_accounts=max_accounts,
        created_after=created_after,
        created_before=created_before,
        sort_by=sort_by or "created_at",
        sort_order=sort_order or "desc",
        include_aggregations=include_aggregations,
        limit=limit,
        offset=offset,
    )
    return await service.search_users(query, use_cache=False)

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
                "user_with_accounts": {
                    "summary": "Full User with Accounts",
                    "description": "User creation with department, role, phone, and initial financial accounts.",
                    "value": {
                        "name": "Sarah Connor",
                        "email": "sarah.connor@example.com",
                        "phone": "+14155552671",
                        "role": "Administrator",
                        "role_id": "1",
                        "department": "Engineering",
                        "department_id": "ENG",
                        "accounts": [
                            {
                                "account_number": "ACC-CORP-1001",
                                "account_type": "corporate",
                                "balance_cents": 15000000,
                                "currency": "USD",
                                "status": "active",
                            },
                            {
                                "account_number": "ACC-SAV-2002",
                                "account_type": "savings",
                                "balance_cents": 5000000,
                                "currency": "USD",
                                "status": "active",
                            },
                            {
                                "account_number": "ACC-OPS-3003",
                                "account_type": "checking",
                                "balance_cents": 2500000,
                                "currency": "EUR",
                                "status": "active",
                            },
                        ],
                    },
                },
                "standard_user": {
                    "summary": "Standard User",
                    "description": "User with department and role without accounts.",
                    "value": {
                        "name": "John Connor",
                        "email": "john.connor@example.com",
                        "phone": "+14155558899",
                        "role": "Manager",
                        "role_id": "2",
                        "department": "Operations",
                        "department_id": "OPS",
                    },
                },
            }
        ),
    ],
    service: UserServiceDep,
) -> UserRead:
    return await service.create_user(payload)


# ------------------------------------------------------------------------------
# 2b. SEED DIVERSE USERS (POST /users/seed)
# ------------------------------------------------------------------------------
@router.post(
    "/seed",
    status_code=status.HTTP_201_CREATED,
    summary="Seed diverse users in bulk",
    description="Generates and inserts high-volume diverse test users with multiple accounts across departments, roles, and currencies in a single raw SQL query.",
)
async def seed_users(
    service: UserServiceDep,
    count: Annotated[int, Query(ge=1, le=5000, description="Number of diverse users to generate and insert")] = 10,
) -> Dict[str, Any]:
    inserted_count = await service.seed_diverse_users(count)
    return {
        "status": "success",
        "seeded_users": inserted_count,
        "message": f"Successfully seeded {inserted_count} diverse users with accounts into PostgreSQL.",
    }


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
# 5. GET USER PROFILE (GET /users/{user_id}/profile)
# ------------------------------------------------------------------------------
@router.get(
    "/{user_id}/profile",
    response_model=UserProfileRead,
    summary="Get aggregated user profile",
    description="Retrieves a complete user profile combining User, Department, Role, and Accounts with 3-tier self-healing (Redis -> ES -> PostgreSQL).",
)
async def get_user_profile(
    user_id: uuid.UUID,
    service: UserServiceDep,
) -> UserProfileRead:
    return await service.get_user_profile(user_id)


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
