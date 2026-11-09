"""User repository: direct asyncpg hand-written SQL queries (no ORM, no BaseRepository inheritance)."""

from datetime import datetime, timezone
from typing import Any, Dict, List, Mapping, Optional, Tuple, cast
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
        department=row.get("department"),
        phone_encrypted=row["phone_encrypted"],
        phone_blind_index=row["phone_blind_index"],
        department_id=row.get("department_id"),
        role_id=row.get("role_id"),
        created_at=row["created_at"],
    )


def _parse_account_dict(acc: Any, user_id: uuid.UUID) -> UserAccount:
    """Safely converts an account mapping or JSON dictionary into a UserAccount dataclass."""
    acc_dict: Mapping[str, Any] = cast(Mapping[str, Any], acc) if isinstance(acc, Mapping) else {}
    raw_id = acc_dict.get("id")
    try:
        acc_id = raw_id if isinstance(raw_id, uuid.UUID) else uuid.UUID(str(raw_id))
    except (ValueError, TypeError):
        acc_id = uuid.uuid4()

    created_at: Optional[datetime] = None
    raw_dt = acc_dict.get("created_at")
    if isinstance(raw_dt, datetime):
        created_at = raw_dt
    elif isinstance(raw_dt, str) and raw_dt.strip():
        try:
            created_at = datetime.fromisoformat(raw_dt.strip())
        except ValueError:
            created_at = None

    return UserAccount(
        id=acc_id,
        user_id=user_id,
        account_number=str(acc_dict.get("account_number", "")),
        account_type=str(acc_dict.get("account_type", "checking")),
        balance_cents=int(acc_dict.get("balance_cents") or 0),
        currency=str(acc_dict.get("currency", "USD")),
        status=str(acc_dict.get("status", "active")),
        created_at=created_at,
    )


def _row_to_profile(row: Any) -> UserProfile:
    user = User(
        id=row["user_id"],
        name=row["user_name"],
        email=row["user_email"],
        role=row["user_role"],
        department=row.get("user_department"),
        phone_encrypted=row["user_phone_encrypted"],
        phone_blind_index=row["user_phone_blind_index"],
        created_at=row["user_created_at"],
    )

    dept: Optional[Department] = None
    if row.get("user_department"):
        dept = Department(
            id=row.get("dept_id"),
            name=row["dept_name"] or row["user_department"],
            code=row["dept_code"] or row["user_department"],
            description=row.get("dept_description"),
        )

    role: Optional[Role] = Role(
        id=row.get("role_id"),
        name=row["role_name"] or row["user_role"],
        description=row.get("role_description"),
        permissions=list(row["role_permissions"] or []),
    )

    accounts_data = row["accounts_json"]
    accounts_list: List[Any]
    if isinstance(accounts_data, (str, bytes)):
        try:
            parsed = orjson.loads(accounts_data)
            accounts_list = cast(List[Any], parsed) if isinstance(parsed, list) else []
        except Exception:
            accounts_list = []
    elif isinstance(accounts_data, list):
        accounts_list = cast(List[Any], accounts_data)
    else:
        accounts_list = []

    accounts: List[UserAccount] = [
        _parse_account_dict(acc, user.id)
        for acc in accounts_list
        if isinstance(acc, (dict, Mapping))
    ]

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
        u.department AS user_department,
        u.phone_encrypted AS user_phone_encrypted,
        u.phone_blind_index AS user_phone_blind_index,
        u.created_at AS user_created_at,
        d.id AS dept_id,
        COALESCE(d.name, u.department) AS dept_name,
        COALESCE(d.code, u.department) AS dept_code,
        d.description AS dept_description,
        r.id AS role_id,
        COALESCE(r.name, u.role) AS role_name,
        r.description AS role_description,
        COALESCE(r.permissions, '{}') AS role_permissions,
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
    LEFT JOIN departments d ON (LOWER(d.name) = LOWER(u.department) OR LOWER(d.code) = LOWER(u.department))
    LEFT JOIN roles r ON LOWER(r.name) = LOWER(u.role)
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
            INSERT INTO users (id, name, email, role, department, phone_encrypted, phone_blind_index, created_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            RETURNING *
        """
        row = await self.db.fetchrow(
            query,
            user.id,
            user.name,
            user.email,
            user.role,
            user.department,
            user.phone_encrypted,
            user.phone_blind_index,
            user.created_at or datetime.now(timezone.utc),
        )
        return _row_to_user(row) if row is not None else user

    async def update(self, user: User) -> User:
        query = """
            UPDATE users
            SET name = $1, email = $2, role = $3, department = $4, phone_encrypted = $5, phone_blind_index = $6
            WHERE id = $7
            RETURNING *
        """
        row = await self.db.fetchrow(
            query,
            user.name,
            user.email,
            user.role,
            user.department,
            user.phone_encrypted,
            user.phone_blind_index,
            user.id,
        )
        return _row_to_user(row) if row is not None else user

    async def delete(self, user_id: uuid.UUID) -> bool:
        await self.db.execute("DELETE FROM user_accounts WHERE user_id = $1", user_id)
        res = await self.db.execute("DELETE FROM users WHERE id = $1", user_id)
        return res != "DELETE 0"

    async def get_existing_account_numbers(self, account_numbers: List[str]) -> List[str]:
        if not account_numbers:
            return []
        rows = await self.db.fetch(
            "SELECT account_number FROM user_accounts WHERE account_number = ANY($1::varchar[])",
            account_numbers,
        )
        return [r["account_number"] for r in rows]

    async def list_keyset(
        self,
        limit: int = 50,
        cursor_dt: Optional[datetime] = None,
        cursor_id: Optional[uuid.UUID] = None,
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

    async def search(
        self,
        query: str,
        limit: int = 20,
        offset: int = 0,
    ) -> List[User]:
        """Hand-written asyncpg SQL search matching name, email, role, or department."""
        safe_limit = min(100, max(1, limit))
        safe_offset = max(0, offset)
        pattern = f"%{query.strip()}%"

        sql = """
            SELECT * FROM users
            WHERE name ILIKE $1 
               OR email ILIKE $1 
               OR role ILIKE $1 
               OR department ILIKE $1
            ORDER BY created_at DESC, id ASC
            LIMIT $2 OFFSET $3
        """
        rows = await self.db.fetch(sql, pattern, safe_limit, safe_offset)
        return [_row_to_user(r) for r in rows]

    async def search_complex(
        self,
        q: Optional[str] = None,
        department: Optional[str] = None,
        role: Optional[str] = None,
        account_type: Optional[str] = None,
        currency: Optional[str] = None,
        min_balance_cents: Optional[int] = None,
        max_balance_cents: Optional[int] = None,
        min_accounts: Optional[int] = None,
        max_accounts: Optional[int] = None,
        created_after: Optional[datetime] = None,
        created_before: Optional[datetime] = None,
        sort_by: Optional[str] = "created_at",
        sort_order: Optional[str] = "desc",
        limit: int = 20,
        offset: int = 0,
    ) -> Tuple[List[Dict[str, Any]], int]:
        """Hand-written asyncpg SQL query with dynamic WHERE, HAVING, aggregation, and window total count."""
        where_clauses: List[str] = []
        having_clauses: List[str] = []
        params: List[Any] = []

        if q and q.strip():
            pattern = f"%{q.strip()}%"
            params.append(pattern)
            p_idx = len(params)
            where_clauses.append(
                f"(u.name ILIKE ${p_idx} OR u.email ILIKE ${p_idx} OR u.role ILIKE ${p_idx} "
                f"OR u.department ILIKE ${p_idx} OR a.account_number ILIKE ${p_idx})"
            )

        if department:
            dept_term = department.strip()
            dept_matches = await self.db.fetch(
                "SELECT name, code FROM departments WHERE LOWER(name) = LOWER($1) OR LOWER(code) = LOWER($1)",
                dept_term,
            )
            dept_values = {dept_term}
            for dm in dept_matches:
                if dm["name"]:
                    dept_values.add(dm["name"])
                if dm["code"]:
                    dept_values.add(dm["code"])
            params.append(list(dept_values))
            p_idx = len(params)
            where_clauses.append(f"u.department = ANY(${p_idx}::text[])")

        if role:
            role_term = role.strip()
            role_matches = await self.db.fetch(
                "SELECT name FROM roles WHERE LOWER(name) = LOWER($1)",
                role_term,
            )
            role_values = {role_term}
            for rm in role_matches:
                if rm["name"]:
                    role_values.add(rm["name"])
            params.append(list(role_values))
            p_idx = len(params)
            where_clauses.append(f"u.role = ANY(${p_idx}::text[])")

        if account_type:
            params.append(account_type.strip())
            p_idx = len(params)
            where_clauses.append(f"LOWER(a.account_type) = LOWER(${p_idx})")

        if currency:
            params.append(currency.strip().upper())
            p_idx = len(params)
            where_clauses.append(f"UPPER(a.currency) = ${p_idx}")

        if created_after:
            params.append(created_after)
            p_idx = len(params)
            where_clauses.append(f"u.created_at >= ${p_idx}")

        if created_before:
            params.append(created_before)
            p_idx = len(params)
            where_clauses.append(f"u.created_at <= ${p_idx}")

        if min_balance_cents is not None:
            params.append(min_balance_cents)
            p_idx = len(params)
            having_clauses.append(f"COALESCE(SUM(a.balance_cents), 0) >= ${p_idx}")

        if max_balance_cents is not None:
            params.append(max_balance_cents)
            p_idx = len(params)
            having_clauses.append(f"COALESCE(SUM(a.balance_cents), 0) <= ${p_idx}")

        if min_accounts is not None:
            params.append(min_accounts)
            p_idx = len(params)
            having_clauses.append(f"COUNT(a.id) >= ${p_idx}")

        if max_accounts is not None:
            params.append(max_accounts)
            p_idx = len(params)
            having_clauses.append(f"COUNT(a.id) <= ${p_idx}")

        where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
        having_sql = f"HAVING {' AND '.join(having_clauses)}" if having_clauses else ""

        safe_order = "ASC" if str(sort_order).lower() == "asc" else "DESC"
        sort_map = {
            "created_at": f"u.created_at {safe_order}, u.id ASC",
            "total_balance_cents": f"total_balance_cents {safe_order}, u.id ASC",
            "account_count": f"account_count {safe_order}, u.id ASC",
            "name": f"u.name {safe_order}, u.id ASC",
            "email": f"u.email {safe_order}, u.id ASC",
        }
        order_by_sql = sort_map.get(str(sort_by).lower(), f"u.created_at {safe_order}, u.id ASC")

        safe_limit = min(100, max(1, limit))
        safe_offset = max(0, offset)

        params.append(safe_limit)
        limit_idx = len(params)
        params.append(safe_offset)
        offset_idx = len(params)

        has_account_filters = bool(having_clauses or account_type or currency or (q and q.strip()))

        if not has_account_filters:
            user_order_map = {
                "created_at": f"u.created_at {safe_order}, u.id ASC",
                "name": f"u.name {safe_order}, u.id ASC",
                "email": f"u.email {safe_order}, u.id ASC",
            }
            user_order_by = user_order_map.get(str(sort_by).lower(), f"u.created_at {safe_order}, u.id ASC")

            # Fast user-first pagination (avoids full-table scan on 10M rows)
            sql = f"""
                WITH user_subset AS (
                    SELECT u.id, u.name, u.email, u.role, u.department, u.phone_encrypted, u.phone_blind_index, u.created_at
                    FROM users u
                    {where_sql}
                    ORDER BY {user_order_by}
                    LIMIT ${limit_idx} OFFSET ${offset_idx}
                )
                SELECT 
                    u.id,
                    u.name,
                    u.email,
                    u.role,
                    u.department,
                    u.phone_encrypted,
                    u.phone_blind_index,
                    u.created_at,
                    d.id AS dept_id,
                    COALESCE(d.name, u.department) AS dept_name,
                    COALESCE(d.code, u.department) AS dept_code,
                    d.description AS dept_description,
                    r.id AS role_id,
                    COALESCE(r.name, u.role) AS role_name,
                    r.description AS role_description,
                    COALESCE(r.permissions, '{{}}') AS role_permissions,
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
                    COALESCE(SUM(a.balance_cents), 0)::bigint AS total_balance_cents,
                    COUNT(a.id)::int AS account_count
                FROM user_subset u
                LEFT JOIN user_accounts a ON a.user_id = u.id
                LEFT JOIN departments d ON (d.name = u.department OR d.code = u.department)
                LEFT JOIN roles r ON r.name = u.role
                GROUP BY u.id, u.name, u.email, u.role, u.department, u.phone_encrypted, u.phone_blind_index, u.created_at, d.id, d.name, d.code, d.description, r.id, r.name, r.description, r.permissions
                ORDER BY {user_order_by}
            """
            rows = await self.db.fetch(sql, *params)
            count_params = params[:limit_idx - 1]
            if where_clauses:
                count_sql = f"SELECT count(*) FROM users u {where_sql}"
                total_val = await self.db.fetchval(count_sql, *count_params)
                total_count = int(total_val or 0)
            else:
                total_val = await self.db.fetchval("SELECT count(*) FROM users")
                total_count = int(total_val or 0)
            items = [dict(r) for r in rows]
            return items, total_count

        sql = f"""
            SELECT 
                u.id,
                u.name,
                u.email,
                u.role,
                u.department,
                u.phone_encrypted,
                u.phone_blind_index,
                u.created_at,
                d.id AS dept_id,
                COALESCE(d.name, u.department) AS dept_name,
                COALESCE(d.code, u.department) AS dept_code,
                d.description AS dept_description,
                r.id AS role_id,
                COALESCE(r.name, u.role) AS role_name,
                r.description AS role_description,
                COALESCE(r.permissions, '{{}}') AS role_permissions,
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
                COALESCE(SUM(a.balance_cents), 0)::bigint AS total_balance_cents,
                COUNT(a.id)::int AS account_count
            FROM users u
            LEFT JOIN user_accounts a ON a.user_id = u.id
            LEFT JOIN departments d ON (d.name = u.department OR d.code = u.department)
            LEFT JOIN roles r ON r.name = u.role
            {where_sql}
            GROUP BY u.id, u.name, u.email, u.role, u.department, u.phone_encrypted, u.phone_blind_index, u.created_at, d.id, d.name, d.code, d.description, r.id, r.name, r.description, r.permissions
            {having_sql}
            ORDER BY {order_by_sql}
            LIMIT ${limit_idx} OFFSET ${offset_idx}
        """

        rows = await self.db.fetch(sql, *params)
        items = [dict(r) for r in rows]
        total_count = len(items) if len(items) < safe_limit and safe_offset == 0 else safe_offset + len(items) + (1 if len(items) == safe_limit else 0)
        return items, total_count

    # --------------------------------------------------------------------------
    # Complex Aggregated Query: User Profile (User + Department + Role + Accounts)
    # --------------------------------------------------------------------------
    async def get_profile(self, user_id: uuid.UUID) -> Optional[UserProfile]:
        """Complex query joining users, departments, roles, and user_accounts on text/string relations."""
        query = f"{PROFILE_BASE_QUERY} WHERE u.id = $1 GROUP BY u.id, d.id, r.id, d.name, d.code, d.description, r.name, r.description, r.permissions"
        row = await self.db.fetchrow(query, user_id)
        return _row_to_profile(row) if row else None

    async def list_all_profiles(self) -> List[UserProfile]:
        """Fetches all user profiles for bulk Elasticsearch synchronization."""
        query = f"{PROFILE_BASE_QUERY} GROUP BY u.id, d.id, r.id, d.name, d.code, d.description, r.name, r.description, r.permissions ORDER BY u.created_at ASC"
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
        if row is None:
            return account
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

    async def create_accounts(self, accounts: List[UserAccount]) -> None:
        """Bulk insert multiple accounts in a single query without looping."""
        if not accounts:
            return
        query = """
            INSERT INTO user_accounts (id, user_id, account_number, account_type, balance_cents, currency, status, created_at, updated_at)
            SELECT * FROM UNNEST(
                $1::uuid[],
                $2::uuid[],
                $3::varchar[],
                $4::varchar[],
                $5::bigint[],
                $6::varchar[],
                $7::varchar[],
                $8::timestamptz[],
                $9::timestamptz[]
            )
        """
        now = datetime.now(timezone.utc)
        await self.db.execute(
            query,
            [a.id for a in accounts],
            [a.user_id for a in accounts],
            [a.account_number for a in accounts],
            [a.account_type for a in accounts],
            [a.balance_cents for a in accounts],
            [a.currency for a in accounts],
            [a.status for a in accounts],
            [a.created_at or now for a in accounts],
            [a.updated_at or now for a in accounts],
        )

    async def seed_users(self, count: int) -> None:
        """Executes PostgreSQL bulk seeder function in a single raw database call without loops."""
        await self.db.execute("SELECT seed_diverse_users($1::int)", count)

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
