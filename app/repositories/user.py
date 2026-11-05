"""User repository: direct asyncpg hand-written SQL queries (no ORM, no BaseRepository inheritance)."""

from datetime import datetime, timezone
from typing import Any, List, Optional, Tuple
import uuid

from app.db.pool import UnitOfWork
from app.models.user import User
from app.repositories.sql_helpers import keyset_seek_clause


def _row_to_user(row: Any) -> User:
    return User(
        id=row["id"],
        name=row["name"],
        email=row["email"],
        role=row["role"],
        phone_encrypted=row["phone_encrypted"],
        phone_blind_index=row["phone_blind_index"],
        created_at=row["created_at"],
    )


class UserRepository:
    """Clean, hand-written asyncpg repository for User entity."""

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
            INSERT INTO users (id, name, email, role, phone_encrypted, phone_blind_index, created_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
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
            user.created_at or datetime.now(timezone.utc),
        )
        return _row_to_user(row)

    async def update(self, user: User) -> User:
        query = """
            UPDATE users
            SET name = $1, email = $2, role = $3, phone_encrypted = $4, phone_blind_index = $5
            WHERE id = $6
            RETURNING *
        """
        row = await self.db.fetchrow(
            query,
            user.name,
            user.email,
            user.role,
            user.phone_encrypted,
            user.phone_blind_index,
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

        seek, seek_params = keyset_seek_clause("created_at", "id", cursor_dt, cursor_id, len(params) + 1)
        if seek:
            clauses.append(seek)
            params.extend(seek_params)

        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        safe_limit = min(100, max(1, limit))
        params.append(safe_limit + 1)
        limit_placeholder = f"${len(params)}"

        query = f"SELECT * FROM users {where} ORDER BY created_at ASC, id ASC LIMIT {limit_placeholder}"
        rows = await self.db.fetch(query, *params)
        has_next = len(rows) > safe_limit
        items = [_row_to_user(r) for r in rows[:safe_limit]]
        return items, has_next


__all__ = ["UserRepository"]
