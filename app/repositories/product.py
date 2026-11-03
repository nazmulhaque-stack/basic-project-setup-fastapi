"""Product repository: entity-specific SQL + sort/filter logic, executed
through BaseRepository's dynamic fetch_one/fetch_all/insert/update_row/
delete_row/execute primitives (shared Redis caching + automatic Elasticsearch
sync)."""

from collections.abc import Mapping, Sequence
from datetime import datetime
from typing import Any, List, Optional, Tuple, Union
import uuid

from app.models.category import Category
from app.models.product import Product
from app.repositories.base import BaseRepository, SQLExpr
from app.repositories.sql_helpers import keyset_seek_clause

_SORTABLE = {"id", "name", "price_cents", "stock", "created_at"}


def _row_to_product(row: Any) -> Product:
    return Product(
        id=row["id"],
        name=row["name"],
        sku=row["sku"],
        price_cents=row["price_cents"],
        category=row["category"],
        category_id=row["category_id"],
        is_active=row["is_active"],
        stock=row["stock"],
        reserved_quantity=row["reserved_quantity"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


class ProductRepository(BaseRepository[Product, uuid.UUID]):
    """Concrete repository for the Product entity."""

    table = "products"
    primary_key = "id"

    @property
    def model(self) -> type[Product]:
        return Product

    async def get(self, entity_id: Union[uuid.UUID, int, str]) -> Optional[Product]:
        row = await self.fetch_one("SELECT * FROM products WHERE id = $1", (entity_id,))
        return _row_to_product(row) if row else None

    async def get_by_sku(self, sku: str) -> Optional[Product]:
        """Finds product by unique SKU. Cached: a hot per-write validation
        lookup (SKU-uniqueness checks on create/update) with no service-layer
        cache wrapping it, unlike the generic `.get(id)` path above."""
        row = await self.fetch_one(
            "SELECT * FROM products WHERE sku = $1", (sku,), cache=True, cache_ttl=120
        )
        return _row_to_product(row) if row else None

    async def get_with_category(self, product_id: Union[uuid.UUID, int, str]) -> Optional[Product]:
        """Eagerly loads product with its category via a second query
        (the hand-written equivalent of `selectinload(Product.category_rel)`)."""
        product = await self.get(product_id)
        if product is None or product.category_id is None:
            return product
        cat_row = await self.fetch_one(
            "SELECT * FROM categories WHERE id = $1", (product.category_id,)
        )
        if cat_row:
            product.category_rel = Category(
                id=cat_row["id"],
                name=cat_row["name"],
                description=cat_row["description"],
                is_active=cat_row["is_active"],
                created_at=cat_row["created_at"],
                updated_at=cat_row["updated_at"],
            )
        return product

    async def change_stock_atomic(self, product_id: Union[uuid.UUID, int, str], delta: int) -> bool:
        """Atomically updates inventory stock without race conditions:
        UPDATE products SET stock = stock + delta WHERE id = :id AND stock + delta >= reserved_quantity.

        Routed through `execute()` (not `update_row()`) because the WHERE
        clause here is a computed inequality, not a plain equality the
        generic dict-based `update_row()` can express — but it still goes
        through the shared engine for query-cache invalidation and
        Elasticsearch auto-sync.
        """
        result = await self.execute(
            """
            UPDATE products
            SET stock = stock + $2, updated_at = now()
            WHERE id = $1 AND stock + $2 >= reserved_quantity
            """,
            (product_id, delta),
            auto_sync=True,
            sync_op="upsert",
            entity_id=product_id,
        )
        # asyncpg execute() returns a status string like "UPDATE 1"
        return result.endswith(" 1")

    async def low_stock(self, threshold: int = 5, limit: int = 50) -> Sequence[Product]:
        """Fetches active products with inventory stock under or equal to threshold."""
        rows = await self.fetch_all(
            """
            SELECT * FROM products
            WHERE is_active = true AND (stock - reserved_quantity) <= $1
            ORDER BY (stock - reserved_quantity) ASC, id ASC
            LIMIT $2
            """,
            (threshold, limit),
        )
        return [_row_to_product(r) for r in rows]

    async def create(self, entity: Product) -> Product:
        result = await self.insert(
            {
                "id": entity.id,
                "name": entity.name,
                "sku": entity.sku,
                "price_cents": entity.price_cents,
                "category": entity.category,
                "category_id": entity.category_id,
                "is_active": entity.is_active,
                "stock": entity.stock,
                "reserved_quantity": entity.reserved_quantity,
            }
        )
        return _row_to_product(result)

    async def update(self, entity: Product) -> Product:
        result = await self.update_row(
            {
                "name": entity.name,
                "sku": entity.sku,
                "price_cents": entity.price_cents,
                "category": entity.category,
                "category_id": entity.category_id,
                "is_active": entity.is_active,
                "stock": entity.stock,
                "reserved_quantity": entity.reserved_quantity,
                "updated_at": SQLExpr("now()"),
            },
            {"id": entity.id},
        )
        return _row_to_product(result)

    async def delete(self, entity: Product) -> None:
        await self.delete_row({"id": entity.id})

    def _build_where(self, filters: Mapping[str, Any]) -> Tuple[str, List[Any]]:
        """Hand-written WHERE builder for exactly the filters ProductsRouter declares."""
        clauses: List[str] = []
        params: List[Any] = []

        def add(clause: str, value: Any) -> None:
            params.append(value)
            clauses.append(clause.format(n=len(params)))

        if filters.get("name__icontains") is not None:
            add("name ILIKE '%' || ${n} || '%'", filters["name__icontains"])
        if filters.get("sku") is not None:
            add("sku = ${n}", filters["sku"])
        if filters.get("sku__startswith") is not None:
            add("sku LIKE ${n} || '%'", filters["sku__startswith"])
        if filters.get("category") is not None:
            add("category = ${n}", filters["category"])
        if filters.get("category__in") is not None:
            add("category = ANY(${n})", list(filters["category__in"]))
        if filters.get("category_id") is not None:
            add("category_id = ${n}", filters["category_id"])
        if filters.get("category_id__in") is not None:
            add("category_id = ANY(${n})", list(filters["category_id__in"]))
        if filters.get("is_active") is not None:
            add("is_active = ${n}", filters["is_active"])
        if filters.get("stock__lte") is not None:
            add("stock <= ${n}", filters["stock__lte"])

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
    ) -> Sequence[Product]:
        where, params = self._build_where(filters or {})
        order_by = self._build_order_by(sort)
        safe_limit = min(100, max(1, limit))
        query = f"SELECT * FROM products {where} {order_by} OFFSET ${len(params) + 1} LIMIT ${len(params) + 2}"
        rows = await self.fetch_all(query, (*params, max(0, offset), safe_limit))
        return [_row_to_product(r) for r in rows]

    async def count(self, filters: Optional[Mapping[str, Any]] = None) -> int:
        where, params = self._build_where(filters or {})
        row = await self.fetch_one(f"SELECT count(*) AS count FROM products {where}", params)
        return int(row["count"]) if row else 0

    async def list_keyset(
        self,
        limit: int = 50,
        cursor_dt: Optional[datetime] = None,
        cursor_id: Optional[Any] = None,
        order_col: str = "created_at",
        id_col: str = "id",
        **filters: Any,
    ) -> Tuple[List[Product], bool]:
        where, params = self._build_where(filters)
        seek, seek_params = keyset_seek_clause(order_col, id_col, cursor_dt, cursor_id, len(params) + 1)
        if seek:
            where = f"{where} AND {seek}" if where else f"WHERE {seek}"
            params += seek_params

        safe_limit = min(100, max(1, limit))
        query = (
            f"SELECT * FROM products {where} "
            f"ORDER BY {order_col} ASC, {id_col} ASC LIMIT ${len(params) + 1}"
        )
        rows = await self.fetch_all(query, (*params, safe_limit + 1))
        has_next = len(rows) > safe_limit
        items = [_row_to_product(r) for r in rows[:safe_limit]]
        return items, has_next


__all__ = ["ProductRepository"]
