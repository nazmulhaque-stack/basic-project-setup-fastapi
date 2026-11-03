"""Dynamic hybrid repository: shared asyncpg execution + per-call Redis
caching + automatic outbox-driven Elasticsearch sync.

Concrete repositories (`UserRepository`, `ProductRepository`, ...) still own
every entity-specific SQL string (columns, `_build_where`/`_build_order_by`)
— this project does not build a generic SQL *parser* that tries to infer
filters/joins from arbitrary input, since that would be unsafe. What this
class centralizes is *how* a ready-made parameterized query gets executed:
connection access, row->dict mapping, optional cache-aside reads
(`fetch_one`/`fetch_all`), and optional automatic Elasticsearch
synchronization on writes (`insert`/`update_row`/`delete_row`), so every
repository gets the same caching/sync behavior for free instead of
hand-rolling it per entity.

Caching: per-call `cache=True` on `fetch_one`/`fetch_all` caches that exact
(query, params) pair under a flat hashed key (`cache:<prefix>:query:<hash>`)
for that call's own `cache_ttl` — one `ICacheService.get()` per read, no
extra round trip to resolve a version first. Staleness is therefore bounded
by `cache_ttl`, not actively invalidated: every write (`insert`/`update_row`/
`delete_row`) still bumps a per-table counter (`ICacheService.incr`) as an
explicit cache-churn signal, but nothing reads it back to compute cache keys.
Anything that needs read-your-own-write freshness should use the item/list
caches in `app/services/cache.py` (`DynamicRedisCache`), which *do* actively
invalidate in O(1) via namespace versioning — this generic query cache is
for ad-hoc/custom reads where a bounded TTL is an acceptable tradeoff.

Elasticsearch sync: a write with `auto_sync=True` (the default) looks up the
table in `app.core.elasticsearch_registry.ELASTICSEARCH_INDEXES`; if
registered and enabled, it stages a row in the existing `outbox_events` table
(topic `"elasticsearch.sync"`) in the *same*, still-open transaction as the
write — never a direct dual write to Elasticsearch inside the transaction.
`ElasticsearchSyncDispatcher` (`app/services/elasticsearch_sync.py`) is the
background consumer that actually applies these to the cluster, with the
same FIFO-per-aggregate + `FOR UPDATE SKIP LOCKED` guarantees the Kafka
outbox dispatcher uses, so a delete can never be overtaken by an
earlier-queued update for the same row.
"""

from datetime import datetime
import hashlib
import logging
from typing import Any, Dict, Generic, List, Optional, Sequence, Tuple, TypeVar
import uuid

import orjson

from app.core.cache_interface import ICacheService, NullCache
from app.core.elasticsearch import IElasticsearchService, NullElasticsearch
from app.core.elasticsearch_registry import get_index_config
from app.db.pool import UnitOfWork
from app.models.base import Base
from app.repositories.interface import IBaseRepository

logger = logging.getLogger(__name__)


def _json_safe(value: Any) -> Any:
    """Recursively converts a raw asyncpg row's values into types the
    `outbox_events.payload` JSONB codec (orjson, see `app/db/pool.py`) can
    actually serialize. `dict(row)` from an asyncpg `Record` can contain
    `asyncpg.pgproto.pgproto.UUID` — a UUID-*like* type, but not the exact
    `uuid.UUID` class orjson has built-in native support for, so it still
    raises `TypeError: Type is not JSON serializable`. Everything that isn't
    already a plain JSON-native type or a real `datetime` (which orjson does
    serialize natively) is stringified.
    """
    if isinstance(value, dict):
        return {k: _json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(v) for v in value]
    if value is None or isinstance(value, (str, int, float, bool, datetime)):
        return value
    return str(value)

ModelType = TypeVar("ModelType", bound=Base)
IdType = TypeVar("IdType")

# Dedicated outbox topic for Elasticsearch sync events — kept separate from
# the Kafka domain-events topic (`domain.events`) so the two dispatchers never
# compete for the same rows.
ES_SYNC_TOPIC = "elasticsearch.sync"


class SQLExpr:
    """Wraps a raw SQL expression (e.g. `now()`) so `insert()`/`update_row()`
    inline it literally into the generated statement instead of binding it as
    an ordinary `$N` parameter — asyncpg has no way to bind a string like
    `"now()"` to a `timestamptz` column; Postgres would try (and fail) to
    parse it as a timestamp literal. Only ever construct this with a fixed,
    hand-written SQL fragment — never with request-controlled input, since
    its contents are inlined unparameterized.
    """

    __slots__ = ("sql",)

    def __init__(self, sql: str) -> None:
        self.sql = sql

    def __repr__(self) -> str:
        return f"SQLExpr({self.sql!r})"


class BaseRepository(IBaseRepository[ModelType, IdType], Generic[ModelType, IdType]):
    """Shared dynamic execution engine for hand-written asyncpg repositories.

    Subclasses set two class attributes:
      - `table`: the SQL table name (e.g. `"products"`).
      - `primary_key`: the primary key column (defaults to `"id"`).
    """

    table: str = ""
    primary_key: str = "id"
    cache_prefix: str = ""

    def __init__(
        self,
        db: UnitOfWork,
        cache: Optional[ICacheService] = None,
        elasticsearch: Optional[IElasticsearchService] = None,
    ) -> None:
        self.db: UnitOfWork = db
        self.session: UnitOfWork = db
        # Polymorphic: NullCache/NullElasticsearch make every call below a
        # safe no-op when this repository wasn't constructed through the
        # dependency-injected service layer (e.g. a worker script, a test).
        self.cache: ICacheService = cache if cache is not None else NullCache()
        self.elasticsearch: IElasticsearchService = (
            elasticsearch if elasticsearch is not None else NullElasticsearch()
        )

    def _prefix(self) -> str:
        return self.cache_prefix or self.table or self.model.__name__.lower()

    # --------------------------------------------------------------------------
    # Dynamic query cache (per-call, TTL-bounded)
    # --------------------------------------------------------------------------

    def _query_version_key(self) -> str:
        return f"cache:{self._prefix()}:query_ver"

    def _query_cache_key(self, query: str, params: Sequence[Any]) -> str:
        """Flat hash of (table, query, params) — one `cache.get()` per read,
        no separate round trip to resolve a version number first. Staleness
        is therefore bounded by each call's own `cache_ttl`, not actively
        invalidated: tracking every previously-cached (query, params) key
        well enough to delete exactly those on a write would need either an
        unbounded per-table key set or a second read per lookup (to learn
        "the current version") — not worth it for a cache whose caller
        already chose its own TTL per call. The item/list caches in
        `app/services/cache.py` (`DynamicRedisCache`), which *do* actively
        invalidate in O(1), remain the ones to use for anything that needs
        read-your-own-write freshness.
        """
        normalized = orjson.dumps(
            {"q": " ".join(query.split()), "p": [str(p) for p in params]},
            option=orjson.OPT_SORT_KEYS,
        )
        digest = hashlib.sha256(normalized).hexdigest()[:24]
        return f"cache:{self._prefix()}:query:{digest}"

    async def _invalidate_query_cache(self) -> None:
        """Bumps a per-table write counter on every write. Not read back to
        build cache keys (see `_query_cache_key`) — it's a cheap, explicit
        signal of cache churn for observability/metrics, and a hook a
        version-aware cache client could key off later without changing this
        method's call sites.
        """
        await self.cache.incr(self._query_version_key())

    # --------------------------------------------------------------------------
    # Generic read primitives
    # --------------------------------------------------------------------------

    async def fetch_one(
        self,
        query: str,
        params: Optional[Sequence[Any]] = None,
        *,
        cache: bool = False,
        cache_ttl: int = 300,
    ) -> Optional[Dict[str, Any]]:
        """Runs a parameterized `SELECT` expected to return at most one row.

        `cache=True` caches the (query, params) pair itself — not tied to any
        specific row ID — for `cache_ttl` seconds (±10% is not applied here;
        this is a raw per-call TTL, unlike the service layer's jittered
        item/list TTLs).
        """
        bound_params: Sequence[Any] = tuple(params or ())
        cache_key: Optional[str] = None
        if cache:
            cache_key = self._query_cache_key(query, bound_params)
            cached = await self.cache.get(cache_key)
            if cached is not None:
                return cached.get("row")  # type: ignore[union-attr]

        row = await self.db.fetchrow(query, *bound_params)
        result = dict(row) if row is not None else None

        if cache and cache_key is not None:
            await self.cache.set(cache_key, {"row": result}, ttl=cache_ttl)
        return result

    async def fetch_all(
        self,
        query: str,
        params: Optional[Sequence[Any]] = None,
        *,
        cache: bool = False,
        cache_ttl: int = 300,
    ) -> List[Dict[str, Any]]:
        """Runs a parameterized `SELECT` and returns every matching row as a dict."""
        bound_params: Sequence[Any] = tuple(params or ())
        cache_key: Optional[str] = None
        if cache:
            cache_key = self._query_cache_key(query, bound_params)
            cached = await self.cache.get(cache_key)
            if cached is not None:
                return cached.get("rows", [])  # type: ignore[union-attr]

        rows = await self.db.fetch(query, *bound_params)
        result = [dict(r) for r in rows]

        if cache and cache_key is not None:
            await self.cache.set(cache_key, {"rows": result}, ttl=cache_ttl)
        return result

    # --------------------------------------------------------------------------
    # Generic write primitives
    # --------------------------------------------------------------------------

    async def execute(
        self,
        query: str,
        params: Optional[Sequence[Any]] = None,
        *,
        auto_sync: bool = False,
        sync_op: str = "upsert",
        entity_id: Optional[Any] = None,
        entity_data: Optional[Dict[str, Any]] = None,
    ) -> str:
        """Runs an arbitrary parameterized write statement (not a plain INSERT/
        UPDATE/DELETE by primary key — use `insert`/`update_row`/`delete_row`
        for those, which handle cache invalidation and sync automatically).

        Returns asyncpg's status string (e.g. `"UPDATE 3"`). Always invalidates
        this table's query cache, since an arbitrary write can affect rows this
        repository has cached. Only enqueues an Elasticsearch sync event when
        the caller explicitly identifies the affected row via `entity_id`
        (there's no way to infer that from an arbitrary SQL string safely).

        If `entity_data` isn't given (e.g. a conditional `UPDATE ... WHERE
        stock + $2 >= reserved_quantity` that only returns a status string,
        not `RETURNING *`), this re-fetches the row generically by primary
        key — within the SAME still-open transaction, so it sees the write
        that was just made — rather than staging an undispatchable sync event
        with no document to index.
        """
        bound_params: Sequence[Any] = tuple(params or ())
        status = await self.db.execute(query, *bound_params)
        await self._invalidate_query_cache()
        if auto_sync and entity_id is not None:
            data = entity_data
            if data is None and sync_op != "delete":
                row = await self.db.fetchrow(
                    f"SELECT * FROM {self.table} WHERE {self.primary_key} = $1", entity_id
                )
                data = dict(row) if row is not None else None
            await self._enqueue_es_sync(sync_op, entity_id, data)
        return status

    async def execute_many(
        self,
        query: str,
        records: Sequence[Sequence[Any]],
    ) -> None:
        """Bulk-executes `query` once per row in `records` in a single network
        round-trip (`asyncpg`'s `executemany`). Always invalidates this
        table's query cache. Does NOT auto-sync to Elasticsearch — a bulk
        write's affected rows/ids aren't known generically from `records`
        alone; call `bulk_sync(ids)` explicitly afterward if needed.
        """
        await self.db.connection.executemany(query, records)
        await self._invalidate_query_cache()

    async def bulk_sync(self, op: str, entity_ids: Sequence[Any]) -> None:
        """Explicitly enqueues Elasticsearch sync events for a batch of IDs
        (e.g. after `execute_many`/`bulk_create`-style writes)."""
        for entity_id in entity_ids:
            await self._enqueue_es_sync(op, entity_id, None)

    async def insert(
        self,
        data: Dict[str, Any],
        *,
        table: Optional[str] = None,
        returning: str = "*",
        auto_sync: bool = True,
    ) -> Dict[str, Any]:
        """Hand-built single-row `INSERT ... RETURNING` from a column->value dict.

        This still emits a fully parameterized statement (`$1`, `$2`, ...) for
        every ordinary value — only column *names* come from `data`'s keys,
        never user-controlled values, and callers pass column names they
        themselves declared (e.g. a dataclass field dump), not raw request
        input. A value wrapped in `SQLExpr` (e.g. `SQLExpr("now()")`) is
        inlined literally instead of bound, for the rare column that needs a
        SQL-side expression rather than a Python value — `SQLExpr` is only
        ever constructed by the developer writing the repository method, with
        a fixed literal, never from request-controlled data.
        """
        tbl = table or self.table
        columns = list(data.keys())
        bound_values: List[Any] = []
        placeholders: List[str] = []
        for col in columns:
            value = data[col]
            if isinstance(value, SQLExpr):
                placeholders.append(value.sql)
            else:
                bound_values.append(value)
                placeholders.append(f"${len(bound_values)}")
        query = (
            f"INSERT INTO {tbl} ({', '.join(columns)}) "
            f"VALUES ({', '.join(placeholders)}) RETURNING {returning}"
        )
        row = await self.db.fetchrow(query, *bound_values)
        result = dict(row)
        await self._invalidate_query_cache()
        if auto_sync:
            await self._enqueue_es_sync("upsert", result.get(self.primary_key), result)
        return result

    async def update_row(
        self,
        data: Dict[str, Any],
        where: Dict[str, Any],
        *,
        table: Optional[str] = None,
        returning: str = "*",
        auto_sync: bool = True,
    ) -> Optional[Dict[str, Any]]:
        """Hand-built single-row `UPDATE ... WHERE ... RETURNING` from column->value
        dicts. Supports `SQLExpr` values in `data` the same way `insert()` does
        (e.g. `{"updated_at": SQLExpr("now()")}`)."""
        tbl = table or self.table
        bound_values: List[Any] = []
        set_parts: List[str] = []
        for col, value in data.items():
            if isinstance(value, SQLExpr):
                set_parts.append(f"{col} = {value.sql}")
            else:
                bound_values.append(value)
                set_parts.append(f"{col} = ${len(bound_values)}")

        where_parts: List[str] = []
        for col, value in where.items():
            bound_values.append(value)
            where_parts.append(f"{col} = ${len(bound_values)}")

        query = f"UPDATE {tbl} SET {', '.join(set_parts)} WHERE {' AND '.join(where_parts)} RETURNING {returning}"
        row = await self.db.fetchrow(query, *bound_values)
        await self._invalidate_query_cache()
        if row is None:
            return None
        result = dict(row)
        if auto_sync:
            await self._enqueue_es_sync("upsert", result.get(self.primary_key), result)
        return result

    async def delete_row(
        self,
        where: Dict[str, Any],
        *,
        table: Optional[str] = None,
        auto_sync: bool = True,
        soft_delete: bool = False,
        soft_delete_column: str = "is_active",
    ) -> int:
        """Hand-built `DELETE ... WHERE ...` (or, with `soft_delete=True`, an
        `UPDATE ... SET <soft_delete_column> = false` instead), returning the
        number of affected rows."""
        tbl = table or self.table
        where_cols = list(where.keys())
        where_clause = " AND ".join(f"{c} = ${i + 1}" for i, c in enumerate(where_cols))

        if soft_delete:
            # A soft delete re-indexes the row (it still exists, just flagged
            # inactive) rather than removing the document, so it needs the
            # full updated row back, not just the primary key.
            query = f"UPDATE {tbl} SET {soft_delete_column} = false WHERE {where_clause} RETURNING *"
        else:
            query = f"DELETE FROM {tbl} WHERE {where_clause} RETURNING {self.primary_key}"

        rows = await self.db.fetch(query, *where.values())
        await self._invalidate_query_cache()
        if auto_sync:
            sync_op = "upsert" if soft_delete else "delete"
            for row in rows:
                row_data = dict(row) if soft_delete else None
                await self._enqueue_es_sync(sync_op, row[self.primary_key], row_data)
        return len(rows)

    # --------------------------------------------------------------------------
    # Generic CRUD built on the primitives above — directly usable for any
    # table that only needs lookup-by-primary-key filtering. An entity with
    # richer filter/sort needs (User/Product/Category/Employee) overrides
    # these with its own typed, dataclass-returning versions plus its own
    # `_build_where`/`_build_order_by` — these generic defaults exist so a
    # new, simple table doesn't need that boilerplate at all: set `table`/
    # `primary_key`/`model` and the dynamic engine does the rest.
    # --------------------------------------------------------------------------

    async def get(self, entity_id: Any) -> Optional[Dict[str, Any]]:
        """Generic get-by-primary-key, returning a raw row dict."""
        return await self.fetch_one(
            f"SELECT * FROM {self.table} WHERE {self.primary_key} = $1", (entity_id,)
        )

    async def create(self, data: Dict[str, Any]) -> Dict[str, Any]:  # type: ignore[override]
        """Generic create from a column->value dict (see `insert()`)."""
        return await self.insert(data)

    async def update(self, entity_id: Any, data: Dict[str, Any]) -> Optional[Dict[str, Any]]:  # type: ignore[override]
        """Generic update-by-primary-key from a column->value dict (see `update_row()`)."""
        return await self.update_row(data, {self.primary_key: entity_id})

    async def delete(self, entity_id: Any) -> int:  # type: ignore[override]
        """Generic delete-by-primary-key (see `delete_row()`)."""
        return await self.delete_row({self.primary_key: entity_id})

    async def list(
        self,
        *,
        filters: Optional[Dict[str, Any]] = None,
        sort: Optional[Sequence[str]] = None,
        offset: int = 0,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        """Generic offset-paginated listing with no filters (a table with
        filter/sort needs overrides this with its own `_build_where`)."""
        safe_limit = min(100, max(1, limit))
        query = f"SELECT * FROM {self.table} OFFSET $1 LIMIT $2"
        return await self.fetch_all(query, (max(0, offset), safe_limit))

    async def count(self, filters: Optional[Dict[str, Any]] = None) -> int:
        """Generic unfiltered row count."""
        row = await self.fetch_one(f"SELECT count(*) AS count FROM {self.table}")
        return int(row["count"]) if row else 0

    async def list_keyset(
        self,
        limit: int = 50,
        cursor_dt: Optional[Any] = None,
        cursor_id: Optional[Any] = None,
        order_col: str = "created_at",
        id_col: str = "id",
        **filters: Any,
    ) -> Tuple[List[Dict[str, Any]], bool]:
        """Generic keyset (cursor) pagination with no filters."""
        from app.repositories.sql_helpers import keyset_seek_clause

        seek, seek_params = keyset_seek_clause(order_col, id_col, cursor_dt, cursor_id, 1)
        where = f"WHERE {seek}" if seek else ""
        safe_limit = min(100, max(1, limit))
        query = (
            f"SELECT * FROM {self.table} {where} "
            f"ORDER BY {order_col} ASC, {id_col} ASC LIMIT ${len(seek_params) + 1}"
        )
        rows = await self.fetch_all(query, (*seek_params, safe_limit + 1))
        has_next = len(rows) > safe_limit
        return rows[:safe_limit], has_next

    # --------------------------------------------------------------------------
    # Elasticsearch: automatic sync (outbox) + on-demand search
    # --------------------------------------------------------------------------

    async def _enqueue_es_sync(self, op: str, entity_id: Any, data: Optional[Dict[str, Any]]) -> None:
        """Stages an Elasticsearch sync event in the outbox, in the SAME
        (still-open) transaction as the write that triggered it. Never calls
        Elasticsearch directly — `ElasticsearchSyncDispatcher` does that,
        after the transaction has actually committed, which is the whole
        point of routing this through the transactional outbox instead of a
        direct dual write.
        """
        if entity_id is None:
            return
        cfg = get_index_config(self.table)
        if cfg is None or not cfg.get("enabled", True) or not cfg.get("auto_sync", True):
            return

        event_type = f"es_sync.{self.table}.{op}"
        payload: Dict[str, Any] = {
            "op": op,
            "table": self.table,
            "index": cfg["index"],
            "entity_id": str(entity_id),
            "data": _json_safe(data) if data is not None else None,
        }
        await self.db.execute(
            """
            INSERT INTO outbox_events (id, topic, event_key, event_type, payload)
            VALUES ($1, $2, $3, $4, $5)
            """,
            uuid.uuid4(),
            ES_SYNC_TOPIC,
            str(entity_id),
            event_type,
            payload,
        )

    async def search(
        self,
        query: Any,
        *,
        index: Optional[str] = None,
        search_enabled: bool = True,
        limit: int = 20,
        offset: int = 0,
    ) -> Tuple[List[Dict[str, Any]], int]:
        """On-demand Elasticsearch query — never triggered automatically by a
        database read. `search_enabled=False` short-circuits without touching
        the cluster (e.g. a caller that wants the generic repository shape
        but has its own reason to skip search this one time).

        Defaults `index` to `self.table` — not a lookup into
        `ELASTICSEARCH_INDEXES` (that registry only decides *auto-sync*
        destinations for `insert`/`update_row`/`delete_row`, a separate
        concern). Pass `index=` explicitly for anything that needs to query
        wherever auto-sync is actually writing, e.g. `BaseService.search()`'s
        existing `/search` endpoint convention (`self.resource_name.lower()`,
        singular) — the spec's own example repository method
        (`search_products`) does exactly this, passing `index=` itself
        rather than relying on this default.
        """
        if not search_enabled:
            return [], 0
        idx = index or self.table
        return await self.elasticsearch.search(index=idx, query=query, limit=limit, offset=offset)

    # --------------------------------------------------------------------------
    # Transactions
    # --------------------------------------------------------------------------

    def transaction(self) -> Any:
        """Returns a context manager for an explicit nested transaction
        (asyncpg implements nesting as a `SAVEPOINT` automatically, since this
        repository's connection is already inside the request's outer
        transaction opened by `get_db`). Most callers never need this — the
        outer transaction already covers a whole request — but it's here for
        a service method that must partially commit/roll back within one
        request (e.g. "try this, and if it fails fall back to that, but keep
        whatever the first successful step wrote").
        """
        return self.db.connection.transaction()


__all__ = ["BaseRepository", "ES_SYNC_TOPIC", "SQLExpr"]
