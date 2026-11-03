"""Category repository: entity-specific SQL + sort/filter logic, executed
through BaseRepository's dynamic fetch_one/fetch_all/insert/update_row/
delete_row primitives (shared Redis caching + automatic Elasticsearch sync)."""

from collections.abc import Mapping, Sequence
from datetime import datetime
from typing import Any, List, Optional, Tuple

from app.models.category import Category
from app.repositories.base import BaseRepository, SQLExpr
from app.repositories.sql_helpers import keyset_seek_clause

_SORTABLE = {"id", "name", "created_at"}


def _row_to_category(row: Any) -> Category:
    return Category(
        id=row["id"],
        name=row["name"],
        description=row["description"],
        is_active=row["is_active"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


class CategoryRepository(BaseRepository[Category, int]):
    """Concrete repository for the Category entity."""

    table = "categories"
    primary_key = "id"

    @property
    def model(self) -> type[Category]:
        return Category

    async def get(self, entity_id: int) -> Optional[Category]:
        row = await self.fetch_one("SELECT * FROM categories WHERE id = $1", (entity_id,))
        return _row_to_category(row) if row else None

    async def exists_active(self, category_id: int) -> bool:
        """Checks whether a category exists and is currently active
        (used by ProductService to validate `category_id` on create/update).
        Cached: this is a hot per-write validation lookup with no service-layer
        cache wrapping it, unlike the generic `.get(id)` path above."""
        row = await self.fetch_one(
            "SELECT EXISTS(SELECT 1 FROM categories WHERE id = $1 AND is_active = true) AS result",
            (category_id,),
            cache=True,
            cache_ttl=60,
        )
        return bool(row and row["result"])

    async def create(self, entity: Category) -> Category:
        result = await self.insert(
            {"name": entity.name, "description": entity.description, "is_active": entity.is_active}
        )
        return _row_to_category(result)

    async def update(self, entity: Category) -> Category:
        result = await self.update_row(
            {
                "name": entity.name,
                "description": entity.description,
                "is_active": entity.is_active,
                "updated_at": SQLExpr("now()"),
            },
            {"id": entity.id},
        )
        return _row_to_category(result)

    async def delete(self, entity: Category) -> None:
        await self.delete_row({"id": entity.id})

    def _build_where(self, filters: Mapping[str, Any]) -> Tuple[str, List[Any]]:
        """Hand-written WHERE builder for exactly the filters CategoriesRouter declares."""
        clauses: List[str] = []
        params: List[Any] = []

        def add(clause: str, value: Any) -> None:
            params.append(value)
            clauses.append(clause.format(n=len(params)))

        if filters.get("name") is not None:
            add("name = ${n}", filters["name"])
        if filters.get("name__icontains") is not None:
            add("name ILIKE '%' || ${n} || '%'", filters["name__icontains"])
        if filters.get("is_active") is not None:
            add("is_active = ${n}", filters["is_active"])

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
    ) -> Sequence[Category]:
        where, params = self._build_where(filters or {})
        order_by = self._build_order_by(sort)
        safe_limit = min(100, max(1, limit))
        query = f"SELECT * FROM categories {where} {order_by} OFFSET ${len(params) + 1} LIMIT ${len(params) + 2}"
        rows = await self.fetch_all(query, (*params, max(0, offset), safe_limit))
        return [_row_to_category(r) for r in rows]

    async def count(self, filters: Optional[Mapping[str, Any]] = None) -> int:
        where, params = self._build_where(filters or {})
        row = await self.fetch_one(f"SELECT count(*) AS count FROM categories {where}", params)
        return int(row["count"]) if row else 0

    async def list_keyset(
        self,
        limit: int = 50,
        cursor_dt: Optional[datetime] = None,
        cursor_id: Optional[Any] = None,
        order_col: str = "created_at",
        id_col: str = "id",
        **filters: Any,
    ) -> Tuple[List[Category], bool]:
        where, params = self._build_where(filters)
        seek, seek_params = keyset_seek_clause(order_col, id_col, cursor_dt, cursor_id, len(params) + 1)
        if seek:
            where = f"{where} AND {seek}" if where else f"WHERE {seek}"
            params += seek_params

        safe_limit = min(100, max(1, limit))
        query = (
            f"SELECT * FROM categories {where} "
            f"ORDER BY {order_col} ASC, {id_col} ASC LIMIT ${len(params) + 1}"
        )
        rows = await self.fetch_all(query, (*params, safe_limit + 1))
        has_next = len(rows) > safe_limit
        items = [_row_to_category(r) for r in rows[:safe_limit]]
        return items, has_next


__all__ = ["CategoryRepository"]
