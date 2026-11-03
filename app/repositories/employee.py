"""Employee repository: entity-specific SQL + sort/filter logic, executed
through BaseRepository's dynamic fetch_one/fetch_all/insert/update_row/
delete_row primitives (shared Redis caching + automatic Elasticsearch sync)."""

from collections.abc import Mapping, Sequence
from datetime import datetime
from typing import Any, List, Optional, Tuple
import uuid

from app.models.employee import Employee
from app.repositories.base import BaseRepository
from app.repositories.sql_helpers import keyset_seek_clause

_SORTABLE = {"id", "name", "age", "location", "created_at"}


def _row_to_employee(row: Any) -> Employee:
    return Employee(
        id=row["id"],
        name=row["name"],
        age=row["age"],
        location=row["location"],
        user_id=row["user_id"],
        created_at=row["created_at"],
    )


class EmployeeRepository(BaseRepository[Employee, uuid.UUID]):
    """Concrete repository for the Employee entity."""

    table = "employees"
    primary_key = "id"

    @property
    def model(self) -> type[Employee]:
        return Employee

    async def get(self, entity_id: uuid.UUID) -> Optional[Employee]:
        row = await self.fetch_one("SELECT * FROM employees WHERE id = $1", (entity_id,))
        return _row_to_employee(row) if row else None

    async def get_by_user_id(self, user_id: uuid.UUID) -> Optional[Employee]:
        """Fetch employee by associated user_id (enforces the 1-to-1 constraint).
        Cached: a hot per-write validation lookup with no service-layer cache
        wrapping it, unlike the generic `.get(id)` path above."""
        row = await self.fetch_one(
            "SELECT * FROM employees WHERE user_id = $1", (user_id,), cache=True, cache_ttl=120
        )
        return _row_to_employee(row) if row else None

    async def create(self, entity: Employee) -> Employee:
        result = await self.insert(
            {
                "id": entity.id,
                "name": entity.name,
                "age": entity.age,
                "location": entity.location,
                "user_id": entity.user_id,
            }
        )
        return _row_to_employee(result)

    async def update(self, entity: Employee) -> Employee:
        result = await self.update_row(
            {
                "name": entity.name,
                "age": entity.age,
                "location": entity.location,
                "user_id": entity.user_id,
            },
            {"id": entity.id},
        )
        return _row_to_employee(result)

    async def delete(self, entity: Employee) -> None:
        await self.delete_row({"id": entity.id})

    def _build_where(self, filters: Mapping[str, Any]) -> Tuple[str, List[Any]]:
        """Hand-written WHERE builder for exactly the filters EmployeesRouter declares."""
        clauses: List[str] = []
        params: List[Any] = []

        def add(clause: str, value: Any) -> None:
            params.append(value)
            clauses.append(clause.format(n=len(params)))

        if filters.get("name") is not None:
            add("name = ${n}", filters["name"])
        if filters.get("name__icontains") is not None:
            add("name ILIKE '%' || ${n} || '%'", filters["name__icontains"])
        if filters.get("location") is not None:
            add("location = ${n}", filters["location"])
        if filters.get("location__icontains") is not None:
            add("location ILIKE '%' || ${n} || '%'", filters["location__icontains"])
        if filters.get("age") is not None:
            add("age = ${n}", filters["age"])
        if filters.get("age__gte") is not None:
            add("age >= ${n}", filters["age__gte"])
        if filters.get("age__lte") is not None:
            add("age <= ${n}", filters["age__lte"])
        if filters.get("user_id") is not None:
            add("user_id = ${n}", filters["user_id"])

        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        return where, params

    def _build_order_by(self, sort: Optional[Sequence[str]]) -> str:
        keys = list(sort or ("name",))
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
    ) -> Sequence[Employee]:
        where, params = self._build_where(filters or {})
        order_by = self._build_order_by(sort)
        safe_limit = min(100, max(1, limit))
        query = f"SELECT * FROM employees {where} {order_by} OFFSET ${len(params) + 1} LIMIT ${len(params) + 2}"
        rows = await self.fetch_all(query, (*params, max(0, offset), safe_limit))
        return [_row_to_employee(r) for r in rows]

    async def count(self, filters: Optional[Mapping[str, Any]] = None) -> int:
        where, params = self._build_where(filters or {})
        row = await self.fetch_one(f"SELECT count(*) AS count FROM employees {where}", params)
        return int(row["count"]) if row else 0

    async def list_keyset(
        self,
        limit: int = 50,
        cursor_dt: Optional[datetime] = None,
        cursor_id: Optional[Any] = None,
        order_col: str = "created_at",
        id_col: str = "id",
        **filters: Any,
    ) -> Tuple[List[Employee], bool]:
        where, params = self._build_where(filters)
        seek, seek_params = keyset_seek_clause(order_col, id_col, cursor_dt, cursor_id, len(params) + 1)
        if seek:
            where = f"{where} AND {seek}" if where else f"WHERE {seek}"
            params += seek_params

        safe_limit = min(100, max(1, limit))
        query = (
            f"SELECT * FROM employees {where} "
            f"ORDER BY {order_col} ASC, {id_col} ASC LIMIT ${len(params) + 1}"
        )
        rows = await self.fetch_all(query, (*params, safe_limit + 1))
        has_next = len(rows) > safe_limit
        items = [_row_to_employee(r) for r in rows[:safe_limit]]
        return items, has_next


__all__ = ["EmployeeRepository"]
