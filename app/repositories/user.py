"""User repository: entity-specific SQL + sort/filter logic, executed through
BaseRepository's dynamic fetch_one/fetch_all/insert/update_row/delete_row
primitives (shared Redis caching + automatic Elasticsearch sync)."""

from collections.abc import Mapping, Sequence
from datetime import datetime
from typing import Any, List, Optional, Tuple
import uuid

from app.models.user import User
from app.repositories.base import BaseRepository
from app.repositories.sql_helpers import keyset_seek_clause

_SORTABLE = {"id", "name", "email", "role", "created_at"}


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


class UserRepository(BaseRepository[User, uuid.UUID]):
    """Concrete repository for the User entity."""

    table = "users"
    primary_key = "id"

    @property
    def model(self) -> type[User]:
        return User

    async def get(self, entity_id: uuid.UUID) -> Optional[User]:
        row = await self.fetch_one("SELECT * FROM users WHERE id = $1", (entity_id,))
        return _row_to_user(row) if row else None

    async def get_by_email(self, email: str) -> Optional[User]:
        """Finds user by unique email address. Cached: a hot per-write
        validation lookup with no service-layer cache wrapping it, unlike the
        generic `.get(id)` path above."""
        row = await self.fetch_one(
            "SELECT * FROM users WHERE email = $1", (email,), cache=True, cache_ttl=120
        )
        return _row_to_user(row) if row else None

    async def get_by_phone_blind_index(self, blind_index: str) -> Optional[User]:
        """Finds user by deterministic blind index of encrypted phone. Cached
        for the same reason as `get_by_email`."""
        row = await self.fetch_one(
            "SELECT * FROM users WHERE phone_blind_index = $1",
            (blind_index,),
            cache=True,
            cache_ttl=120,
        )
        return _row_to_user(row) if row else None

    async def create(self, entity: User) -> User:
        result = await self.insert(
            {
                "id": entity.id,
                "name": entity.name,
                "email": entity.email,
                "role": entity.role,
                "phone_encrypted": entity.phone_encrypted,
                "phone_blind_index": entity.phone_blind_index,
            }
        )
        return _row_to_user(result)

    async def update(self, entity: User) -> User:
        result = await self.update_row(
            {
                "name": entity.name,
                "email": entity.email,
                "role": entity.role,
                "phone_encrypted": entity.phone_encrypted,
                "phone_blind_index": entity.phone_blind_index,
            },
            {"id": entity.id},
        )
        return _row_to_user(result)

    async def delete(self, entity: User) -> None:
        await self.delete_row({"id": entity.id})

    def _build_where(self, filters: Mapping[str, Any]) -> Tuple[str, List[Any]]:
        """Hand-written WHERE builder for exactly the filters UsersRouter declares:
        name__icontains, email, email__icontains, role, role__in."""
        clauses: List[str] = []
        params: List[Any] = []

        def add(clause: str, value: Any) -> None:
            params.append(value)
            clauses.append(clause.format(n=len(params)))

        if filters.get("name__icontains") is not None:
            add("name ILIKE '%' || ${n} || '%'", filters["name__icontains"])
        if filters.get("email") is not None:
            add("email = ${n}", filters["email"])
        if filters.get("email__icontains") is not None:
            add("email ILIKE '%' || ${n} || '%'", filters["email__icontains"])
        if filters.get("role") is not None:
            add("role = ${n}", filters["role"])
        if filters.get("role__in") is not None:
            add("role = ANY(${n})", list(filters["role__in"]))

        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        return where, params

    def _build_order_by(self, sort: Optional[Sequence[str]]) -> str:
        keys = list(sort or ("id",))
        order_parts: List[str] = []
        for key in keys:
            desc = key.startswith("-")
            col = key.lstrip("-+")
            if col in _SORTABLE:
                order_parts.append(f"{col} {'DESC' if desc else 'ASC'}")
        if "id" not in {k.lstrip("-+") for k in keys}:
            order_parts.append("id ASC")
        return "ORDER BY " + ", ".join(order_parts)

    async def list(
        self,
        *,
        filters: Optional[Mapping[str, Any]] = None,
        sort: Optional[Sequence[str]] = None,
        offset: int = 0,
        limit: int = 100,
    ) -> Sequence[User]:
        where, params = self._build_where(filters or {})
        order_by = self._build_order_by(sort)
        safe_limit = min(100, max(1, limit))
        query = f"SELECT * FROM users {where} {order_by} OFFSET ${len(params) + 1} LIMIT ${len(params) + 2}"
        rows = await self.fetch_all(query, (*params, max(0, offset), safe_limit))
        return [_row_to_user(r) for r in rows]

    async def count(self, filters: Optional[Mapping[str, Any]] = None) -> int:
        where, params = self._build_where(filters or {})
        row = await self.fetch_one(f"SELECT count(*) AS count FROM users {where}", params)
        return int(row["count"]) if row else 0

    async def list_keyset(
        self,
        limit: int = 50,
        cursor_dt: Optional[datetime] = None,
        cursor_id: Optional[Any] = None,
        order_col: str = "created_at",
        id_col: str = "id",
        **filters: Any,
    ) -> Tuple[List[User], bool]:
        where, params = self._build_where(filters)
        seek, seek_params = keyset_seek_clause(order_col, id_col, cursor_dt, cursor_id, len(params) + 1)
        if seek:
            where = f"{where} AND {seek}" if where else f"WHERE {seek}"
            params += seek_params

        safe_limit = min(100, max(1, limit))
        query = (
            f"SELECT * FROM users {where} "
            f"ORDER BY {order_col} ASC, {id_col} ASC LIMIT ${len(params) + 1}"
        )
        rows = await self.fetch_all(query, (*params, safe_limit + 1))
        has_next = len(rows) > safe_limit
        items = [_row_to_user(r) for r in rows[:safe_limit]]
        return items, has_next


__all__ = ["UserRepository"]
