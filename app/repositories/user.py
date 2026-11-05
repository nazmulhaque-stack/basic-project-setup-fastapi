"""User repository: direct asyncpg hand-written SQL queries (no ORM, no BaseRepository inheritance)."""

from datetime import datetime, timezone
from typing import Any, List, Optional, Tuple
import uuid

import orjson

from app.db.pool import UnitOfWork
from app.models.user import Department, Role, User, UserAccount, UserProfile


def _row_to_user(row: Any) -> User:
    return User(
        id=row["id"],
        name=row["name"],
        email=row["email"],
        role=row["role"],
        phone_encrypted=row["phone_encrypted"],
        phone_blind_index=row["phone_blind_index"],
        department_id=row.get("department_id"),
        role_id=row.get("role_id"),
        created_at=row["created_at"],
    )


def _row_to_profile(row: Any) -> UserProfile:
    user = User(
        id=row["user_id"],
        name=row["user_name"],
        email=row["user_email"],
        role=row["user_role"],
        phone_encrypted=row["user_phone_encrypted"],
        phone_blind_index=row["user_phone_blind_index"],
        department_id=row["department_id"],
        role_id=row["role_id"],
        created_at=row["user_created_at"],
    )

    dept: Optional[Department] = None
    if row["department_id"] and row["dept_name"]:
        dept = Department(
            id=row["department_id"],
            name=row["dept_name"],
            code=row["dept_code"],
            description=row["dept_description"],
        )

    role: Optional[Role] = None
    if row["role_id"] and row["role_name"]:
        role = Role(
            id=row["role_id"],
            name=row["role_name"],
            description=row["role_description"],
            permissions=list(row["role_permissions"] or []),
        )

    accounts_data = row["accounts_json"]
    if isinstance(accounts_data, str):
        accounts_list = orjson.loads(accounts_data)
    else:
        accounts_list = accounts_data or []

    accounts: List[UserAccount] = []
    for acc in accounts_list:
        accounts.append(
            UserAccount(
                id=uuid.UUID(str(acc["id"])),
                user_id=user.id,
                account_number=acc["account_number"],
                account_type=acc.get("account_type", "checking"),
                balance_cents=int(acc.get("balance_cents", 0)),
                currency=acc.get("currency", "USD"),
                status=acc.get("status", "active"),
                created_at=datetime.fromisoformat(acc["created_at"]) if acc.get("created_at") else None,
            )
        )

    return UserProfile(
        user=user,
        department=dept,
        role_info=role,
        accounts=accounts,
        total_balance_cents=int(row["total_balance_cents"] or 0),
    )


PROFILE_BASE_QUERY = """
    SELECT 
        u.id AS user_id,
        u.name AS user_name,
        u.email AS user_email,
        u.role AS user_role,
        u.phone_encrypted AS user_phone_encrypted,
        u.phone_blind_index AS user_phone_blind_index,
        u.created_at AS user_created_at,
        u.department_id,
        u.role_id,
        d.name AS dept_name,
        d.code AS dept_code,
        d.description AS dept_description,
        r.name AS role_name,
        r.description AS role_description,
        r.permissions AS role_permissions,
        COALESCE(
            json_agg(
                json_build_object(
                    'id', a.id,
                    'account_number', a.account_number,
                    'account_type', a.account_type,
                    'balance_cents', a.balance_cents,
                    'currency', a.currency,
                    'status', a.status,
                    'created_at', a.created_at
                )
            ) FILTER (WHERE a.id IS NOT NULL),
            '[]'::json
        ) AS accounts_json,
        COALESCE(SUM(a.balance_cents), 0)::bigint AS total_balance_cents
    FROM users u
    LEFT JOIN departments d ON d.id = u.department_id
    LEFT JOIN roles r ON r.id = u.role_id
    LEFT JOIN user_accounts a ON a.user_id = u.id
"""


class UserRepository:
    """Clean, hand-written asyncpg repository for User entity and aggregated UserProfile."""

    def __init__(self, db: UnitOfWork) -> None:
        self.db = db

    async def get(self, user_id: uuid.UUID) -> Optional[User]:
        row = await self.db.fetchrow("SELECT * FROM users WHERE id = $1", user_id)
        return _row_to_user(row) if row else None

    async def get_by_email(self, email: str) -> Optional[User]:
        row = await self.db.fetchrow("SELECT * FROM users WHERE email = $1", email)
        return _row_to_user(row) if row else None

    async def get_by_phone_blind_index(self, blind_index: str) -> Optional[User]:
        row = await self.db.fetchrow("SELECT * FROM users WHERE phone_blind_index = $1", blind_index)
        return _row_to_user(row) if row else None

    async def create(self, user: User) -> User:
        query = """
            INSERT INTO users (id, name, email, role, phone_encrypted, phone_blind_index, department_id, role_id, created_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            RETURNING *
        """
        row = await self.db.fetchrow(
            query,
            user.id,
            user.name,
            user.email,
            user.role,
            user.phone_encrypted,
            user.phone_blind_index,
            user.department_id,
            user.role_id,
            user.created_at or datetime.now(timezone.utc),
        )
        return _row_to_user(row)

    async def update(self, user: User) -> User:
        query = """
            UPDATE users
            SET name = $1, email = $2, role = $3, phone_encrypted = $4, phone_blind_index = $5, department_id = $6, role_id = $7
            WHERE id = $8
            RETURNING *
        """
        row = await self.db.fetchrow(
            query,
            user.name,
            user.email,
            user.role,
            user.phone_encrypted,
            user.phone_blind_index,
            user.department_id,
            user.role_id,
            user.id,
        )
        return _row_to_user(row)

    async def delete(self, user_id: uuid.UUID) -> bool:
        res = await self.db.execute("DELETE FROM users WHERE id = $1", user_id)
        return res != "DELETE 0"

    async def list_keyset(
        self,
        limit: int = 50,
        cursor_dt: Optional[datetime] = None,
        cursor_id: Optional[Any] = None,
        role: Optional[str] = None,
    ) -> Tuple[List[User], bool]:
        clauses: List[str] = []
        params: List[Any] = []

        if role:
            params.append(role)
            clauses.append(f"role = ${len(params)}")

        if cursor_dt is not None and cursor_id is not None:
            p1, p2 = len(params) + 1, len(params) + 2
            clauses.append(f"(created_at > ${p1} OR (created_at = ${p1} AND id > ${p2}))")
            params.extend([cursor_dt, cursor_id])

        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        safe_limit = min(100, max(1, limit))
        params.append(safe_limit + 1)
        limit_placeholder = f"${len(params)}"

        query = f"SELECT * FROM users {where} ORDER BY created_at ASC, id ASC LIMIT {limit_placeholder}"
        rows = await self.db.fetch(query, *params)
        has_next = len(rows) > safe_limit
        items = [_row_to_user(r) for r in rows[:safe_limit]]
        return items, has_next

    # --------------------------------------------------------------------------
    # Complex Aggregated Query: User Profile (User + Department + Role + Accounts)
    # --------------------------------------------------------------------------
    async def get_profile(self, user_id: uuid.UUID) -> Optional[UserProfile]:
        """Complex query joining users, departments, roles, and user_accounts in a single round-trip."""
        query = f"{PROFILE_BASE_QUERY} WHERE u.id = $1 GROUP BY u.id, d.id, r.id"
        row = await self.db.fetchrow(query, user_id)
        return _row_to_profile(row) if row else None

    async def list_all_profiles(self) -> List[UserProfile]:
        """Fetches all user profiles for bulk Elasticsearch synchronization."""
        query = f"{PROFILE_BASE_QUERY} GROUP BY u.id, d.id, r.id ORDER BY u.created_at ASC"
        rows = await self.db.fetch(query)
        return [_row_to_profile(r) for r in rows]

    # --------------------------------------------------------------------------
    # User Accounts
    # --------------------------------------------------------------------------
    async def create_account(self, account: UserAccount) -> UserAccount:
        query = """
            INSERT INTO user_accounts (id, user_id, account_number, account_type, balance_cents, currency, status, created_at, updated_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            RETURNING *
        """
        row = await self.db.fetchrow(
            query,
            account.id,
            account.user_id,
            account.account_number,
            account.account_type,
            account.balance_cents,
            account.currency,
            account.status,
            account.created_at or datetime.now(timezone.utc),
            account.updated_at or datetime.now(timezone.utc),
        )
        return UserAccount(
            id=row["id"],
            user_id=row["user_id"],
            account_number=row["account_number"],
            account_type=row["account_type"],
            balance_cents=row["balance_cents"],
            currency=row["currency"],
            status=row["status"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    async def list_departments(self) -> List[Department]:
        rows = await self.db.fetch("SELECT * FROM departments ORDER BY name ASC")
        return [
            Department(
                id=r["id"],
                name=r["name"],
                code=r["code"],
                description=r["description"],
                created_at=r["created_at"],
            )
            for r in rows
        ]

    async def list_roles(self) -> List[Role]:
        rows = await self.db.fetch("SELECT * FROM roles ORDER BY name ASC")
        return [
            Role(
                id=r["id"],
                name=r["name"],
                description=r["description"],
                permissions=list(r["permissions"] or []),
                created_at=r["created_at"],
            )
            for r in rows
        ]


__all__ = ["UserRepository"]
