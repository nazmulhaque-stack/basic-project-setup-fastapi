"""Dynamic Service Executor: High-performance, deeply type-safe raw SQL, Redis caching, and Elasticsearch CQRS engine.

Enables developers to execute raw parameterized SQL queries with asyncpg, while dynamically
orchestrating Redis L1 caching (auto-store, revalidation, negative caching), Elasticsearch CQRS,
and Transactional Outbox atomic consistency with zero boilerplate.
"""

import asyncio
from datetime import datetime, timezone
from enum import Enum
import logging
from typing import (
    Any,
    Callable,
    Dict,
    Generic,
    List,
    Literal,
    Mapping,
    Optional,
    Sequence,
    Type,
    TypeVar,
    Union,
    overload,
)
import uuid

from pydantic import BaseModel

from app.core.cache_interface import ICacheService, NullCache
from app.core.elasticsearch import IElasticsearchService, NullElasticsearch
from app.db.pool import UnitOfWork
from app.models.outbox import OutboxEvent
from app.repositories.outbox import OutboxRepository

logger = logging.getLogger(__name__)

# ------------------------------------------------------------------------------
# Generic Type Variables for Deep Type Safety
# ------------------------------------------------------------------------------
T = TypeVar("T")
TSchema = TypeVar("TSchema", bound=BaseModel)


class QueryIntent(str, Enum):
    """Operational intent guiding the dynamic execution engine."""

    PREDICTABLE_ITEM = "predictable_item"  # Redis L1 (<1ms) -> ES L2 fallback -> PostgreSQL L3
    DYNAMIC_SEARCH = "dynamic_search"      # Elasticsearch fuzzy/BM25 primary -> PostgreSQL fallback
    KEYSET_LIST = "keyset_list"            # Redis list namespace versioning -> DB Keyset SQL
    MUTATION = "mutation"                  # PostgreSQL write + Outbox + Redis purge + ES sync


def _record_to_dict(record: Any) -> Dict[str, Any]:
    """Converts an asyncpg Record, BaseModel, or mapping into a plain Python dictionary."""
    if record is None:
        return {}
    if isinstance(record, BaseModel):
        return record.model_dump()
    if isinstance(record, dict):
        return record
    try:
        return dict(record)
    except Exception:
        return {"data": record}


def _json_safe(value: Any) -> Any:
    """Recursively converts asyncpg row values into JSON-serializable primitives."""
    if isinstance(value, dict):
        return {k: _json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(v) for v in value]
    if isinstance(value, uuid.UUID):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, BaseModel):
        return value.model_dump(mode="json")
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    return str(value)


class DynamicServiceExecutor:
    """Unified engine that executes raw parameterized SQL with dynamic Redis & Elasticsearch orchestration.
    
    Provides 100% deep type-safety with overloads for static autocomplete and validation.
    """

    def __init__(
        self,
        db: UnitOfWork,
        cache: Optional[ICacheService] = None,
        elasticsearch: Optional[IElasticsearchService] = None,
        outbox_repo: Optional[OutboxRepository] = None,
        outbox_worker: Optional[Any] = None,
    ) -> None:
        self.db = db
        self.cache: ICacheService = cache or NullCache()
        self.es: IElasticsearchService = elasticsearch or NullElasticsearch()
        self.outbox_repo = outbox_repo or OutboxRepository(db)
        self.outbox_worker = outbox_worker

    # --------------------------------------------------------------------------
    # Cache Internal Helpers (Supporting both raw keys and entity IDs)
    # --------------------------------------------------------------------------
    async def _cache_get(self, key_or_id: Any) -> Optional[Any]:
        """Gets value from cache, supporting both raw cache keys and entity IDs."""
        key_str = str(key_or_id)
        if ":" in key_str:
            val = await self.cache.get(key_str)
            if val is not None:
                return val
        return await self.cache.get_item(key_or_id)

    async def _cache_set(self, key_or_id: Any, value: Any, ttl: Optional[int] = None) -> bool:
        """Stores value in cache, supporting both raw cache keys and entity IDs."""
        key_str = str(key_or_id)
        if ":" in key_str:
            return await self.cache.set(key_str, value, ttl=ttl)
        return await self.cache.set_item(key_or_id, value)

    async def _cache_delete(self, key_or_id: Any) -> bool:
        """Deletes key from cache, supporting both raw cache keys and entity IDs."""
        key_str = str(key_or_id)
        if ":" in key_str:
            await self.cache.delete(key_str)
        return await self.cache.delete_item(key_or_id)

    async def _cache_set_negative(self, key_or_id: Any) -> bool:
        """Sets negative cache sentinel (30s) to block penetration attacks."""
        key_str = str(key_or_id)
        sentinel = {"__not_found__": True}
        if ":" in key_str:
            return await self.cache.set(key_str, sentinel, ttl=30)
        return await self.cache.set_negative_cache(key_or_id)

    def _is_negative_cache(self, value: Any) -> bool:
        """Checks if cached value is the negative cache sentinel."""
        return isinstance(value, dict) and bool(value.get("__not_found__"))

    # --------------------------------------------------------------------------
    # Overload Signatures for execute_query
    # --------------------------------------------------------------------------

    # 1. When schema_cls is provided and fetch_mode is "one" (default)
    @overload
    async def execute_query(
        self,
        *,
        sql: str,
        params: Sequence[Any] = (),
        fetch_mode: Literal["one"] = "one",
        schema_cls: Type[TSchema],
        to_schema: None = None,
        intent: QueryIntent = QueryIntent.PREDICTABLE_ITEM,
        cache_key: Optional[str] = None,
        cache_action: Literal["get_or_set", "get", "set", "none"] = "get_or_set",
        cache_value: Optional[Any] = None,
        cache_ttl: Optional[int] = None,
        revalidate: bool = False,
        bypass_cache: bool = False,
        negative_cache: bool = True,
        es_index: Optional[str] = None,
        es_action: Literal["get", "search", "none"] = "none",
        es_id: Optional[str] = None,
        es_query: Optional[Union[str, Dict[str, Any]]] = None,
    ) -> Optional[TSchema]: ...

    # 2. When schema_cls is provided and fetch_mode is "all"
    @overload
    async def execute_query(
        self,
        *,
        sql: str,
        params: Sequence[Any] = (),
        fetch_mode: Literal["all"],
        schema_cls: Type[TSchema],
        to_schema: None = None,
        intent: QueryIntent = QueryIntent.PREDICTABLE_ITEM,
        cache_key: Optional[str] = None,
        cache_action: Literal["get_or_set", "get", "set", "none"] = "get_or_set",
        cache_value: Optional[Any] = None,
        cache_ttl: Optional[int] = None,
        revalidate: bool = False,
        bypass_cache: bool = False,
        negative_cache: bool = True,
        es_index: Optional[str] = None,
        es_action: Literal["get", "search", "none"] = "none",
        es_id: Optional[str] = None,
        es_query: Optional[Union[str, Dict[str, Any]]] = None,
    ) -> List[TSchema]: ...

    # 3. When custom to_schema is provided and fetch_mode is "one"
    @overload
    async def execute_query(
        self,
        *,
        sql: str,
        params: Sequence[Any] = (),
        fetch_mode: Literal["one"] = "one",
        schema_cls: None = None,
        to_schema: Callable[[Dict[str, Any]], T],
        intent: QueryIntent = QueryIntent.PREDICTABLE_ITEM,
        cache_key: Optional[str] = None,
        cache_action: Literal["get_or_set", "get", "set", "none"] = "get_or_set",
        cache_value: Optional[Any] = None,
        cache_ttl: Optional[int] = None,
        revalidate: bool = False,
        bypass_cache: bool = False,
        negative_cache: bool = True,
        es_index: Optional[str] = None,
        es_action: Literal["get", "search", "none"] = "none",
        es_id: Optional[str] = None,
        es_query: Optional[Union[str, Dict[str, Any]]] = None,
    ) -> Optional[T]: ...

    # 4. When custom to_schema is provided and fetch_mode is "all"
    @overload
    async def execute_query(
        self,
        *,
        sql: str,
        params: Sequence[Any] = (),
        fetch_mode: Literal["all"],
        schema_cls: None = None,
        to_schema: Callable[[Dict[str, Any]], T],
        intent: QueryIntent = QueryIntent.PREDICTABLE_ITEM,
        cache_key: Optional[str] = None,
        cache_action: Literal["get_or_set", "get", "set", "none"] = "get_or_set",
        cache_value: Optional[Any] = None,
        cache_ttl: Optional[int] = None,
        revalidate: bool = False,
        bypass_cache: bool = False,
        negative_cache: bool = True,
        es_index: Optional[str] = None,
        es_action: Literal["get", "search", "none"] = "none",
        es_id: Optional[str] = None,
        es_query: Optional[Union[str, Dict[str, Any]]] = None,
    ) -> List[T]: ...

    # 5. When no schema is provided and fetch_mode is "one"
    @overload
    async def execute_query(
        self,
        *,
        sql: str,
        params: Sequence[Any] = (),
        fetch_mode: Literal["one"] = "one",
        schema_cls: None = None,
        to_schema: None = None,
        intent: QueryIntent = QueryIntent.PREDICTABLE_ITEM,
        cache_key: Optional[str] = None,
        cache_action: Literal["get_or_set", "get", "set", "none"] = "get_or_set",
        cache_value: Optional[Any] = None,
        cache_ttl: Optional[int] = None,
        revalidate: bool = False,
        bypass_cache: bool = False,
        negative_cache: bool = True,
        es_index: Optional[str] = None,
        es_action: Literal["get", "search", "none"] = "none",
        es_id: Optional[str] = None,
        es_query: Optional[Union[str, Dict[str, Any]]] = None,
    ) -> Optional[Dict[str, Any]]: ...

    # 6. When no schema is provided and fetch_mode is "all"
    @overload
    async def execute_query(
        self,
        *,
        sql: str,
        params: Sequence[Any] = (),
        fetch_mode: Literal["all"],
        schema_cls: None = None,
        to_schema: None = None,
        intent: QueryIntent = QueryIntent.PREDICTABLE_ITEM,
        cache_key: Optional[str] = None,
        cache_action: Literal["get_or_set", "get", "set", "none"] = "get_or_set",
        cache_value: Optional[Any] = None,
        cache_ttl: Optional[int] = None,
        revalidate: bool = False,
        bypass_cache: bool = False,
        negative_cache: bool = True,
        es_index: Optional[str] = None,
        es_action: Literal["get", "search", "none"] = "none",
        es_id: Optional[str] = None,
        es_query: Optional[Union[str, Dict[str, Any]]] = None,
    ) -> List[Dict[str, Any]]: ...

    # 7. When fetch_mode is "val"
    @overload
    async def execute_query(
        self,
        *,
        sql: str,
        params: Sequence[Any] = (),
        fetch_mode: Literal["val"],
        schema_cls: Any = None,
        to_schema: Any = None,
        intent: QueryIntent = QueryIntent.PREDICTABLE_ITEM,
        cache_key: Optional[str] = None,
        cache_action: Literal["get_or_set", "get", "set", "none"] = "get_or_set",
        cache_value: Optional[Any] = None,
        cache_ttl: Optional[int] = None,
        revalidate: bool = False,
        bypass_cache: bool = False,
        negative_cache: bool = True,
        es_index: Optional[str] = None,
        es_action: Literal["get", "search", "none"] = "none",
        es_id: Optional[str] = None,
        es_query: Optional[Union[str, Dict[str, Any]]] = None,
    ) -> Any: ...

    async def execute_query(
        self,
        *,
        sql: str,
        params: Sequence[Any] = (),
        fetch_mode: Literal["one", "all", "val"] = "one",
        schema_cls: Optional[Type[TSchema]] = None,
        to_schema: Optional[Callable[[Dict[str, Any]], Any]] = None,
        intent: QueryIntent = QueryIntent.PREDICTABLE_ITEM,
        cache_key: Optional[str] = None,
        cache_action: Literal["get_or_set", "get", "set", "none"] = "get_or_set",
        cache_value: Optional[Any] = None,
        cache_ttl: Optional[int] = None,
        revalidate: bool = False,
        bypass_cache: bool = False,
        negative_cache: bool = True,
        es_index: Optional[str] = None,
        es_action: Literal["get", "search", "none"] = "none",
        es_id: Optional[str] = None,
        es_query: Optional[Union[str, Dict[str, Any]]] = None,
    ) -> Any:
        """Executes read query with multi-tier before-checks (Redis -> ES -> PostgreSQL) and auto-store/revalidate."""
        
        # ----------------------------------------------------------------------
        # Step 1: Redis L1 "Before-Check" (Exact point read <0.5ms)
        # ----------------------------------------------------------------------
        if not revalidate and not bypass_cache and cache_key and cache_action in ("get", "get_or_set"):
            try:
                cached = await self._cache_get(cache_key)
                if cached is not None:
                    if self._is_negative_cache(cached):
                        return None
                    if fetch_mode == "all" and isinstance(cached, list):
                        return [self._transform_item(item, schema_cls, to_schema) for item in cached]
                    if fetch_mode == "val":
                        if isinstance(cached, dict) and "value" in cached:
                            return cached["value"]
                        return cached
                    return self._transform_item(cached, schema_cls, to_schema)
            except Exception as exc:
                logger.warning("Redis L1 before-check failed for key %s: %s", cache_key, exc)

        # ----------------------------------------------------------------------
        # Step 2: Elasticsearch L2 "Before-Check" (Failsafe before DB hit)
        # ----------------------------------------------------------------------
        if not revalidate and not bypass_cache and es_index and es_action == "get" and es_id:
            try:
                es_doc = await self.es.get_document(index=es_index, id_=str(es_id))
                if es_doc is not None:
                    # Auto-backfill Redis L1 for subsequent sub-ms reads
                    if cache_key and cache_action in ("get_or_set", "set"):
                        try:
                            await self._cache_set(cache_key, es_doc, ttl=cache_ttl)
                        except Exception as exc:
                            logger.warning("Failed to auto-backfill Redis from ES: %s", exc)
                    return self._transform_item(es_doc, schema_cls, to_schema)
            except Exception as exc:
                logger.warning("Elasticsearch L2 before-check failed for %s/%s: %s", es_index, es_id, exc)

        # ----------------------------------------------------------------------
        # Step 3: PostgreSQL Raw SQL Execution (Authoritative Source of Truth)
        # ----------------------------------------------------------------------
        if fetch_mode == "one":
            row = await self.db.fetchrow(sql, *params)
            if row is None:
                if negative_cache and cache_key and not bypass_cache:
                    try:
                        await self._cache_set_negative(cache_key)
                    except Exception:
                        pass
                return None

            row_dict = _record_to_dict(row)
            json_safe_dict = _json_safe(row_dict)

            # Auto-store in Redis
            if cache_key and cache_action in ("get_or_set", "set"):
                store_val = cache_value if cache_value is not None else json_safe_dict
                if isinstance(store_val, BaseModel):
                    store_val = store_val.model_dump(mode="json")
                try:
                    await self._cache_set(cache_key, store_val, ttl=cache_ttl)
                except Exception as exc:
                    logger.warning("Redis auto-store failed for key %s: %s", cache_key, exc)

            # Auto-sync to Elasticsearch if configured
            if es_index and es_action in ("get", "index") and es_id:
                try:
                    await self.es.index_document(index=es_index, id_=str(es_id), document=json_safe_dict)
                except Exception as exc:
                    logger.warning("ES auto-backfill failed for %s/%s: %s", es_index, es_id, exc)

            return self._transform_item(row_dict, schema_cls, to_schema)

        elif fetch_mode == "all":
            rows = await self.db.fetch(sql, *params)
            row_dicts = [_record_to_dict(r) for r in rows]
            json_safe_list = [_json_safe(rd) for rd in row_dicts]

            if cache_key and cache_action in ("get_or_set", "set"):
                store_val = cache_value if cache_value is not None else json_safe_list
                if isinstance(store_val, BaseModel):
                    store_val = store_val.model_dump(mode="json")
                try:
                    await self._cache_set(cache_key, store_val, ttl=cache_ttl)
                except Exception as exc:
                    logger.warning("Redis auto-store list failed for key %s: %s", cache_key, exc)

            return [self._transform_item(rd, schema_cls, to_schema) for rd in row_dicts]

        elif fetch_mode == "val":
            val = await self.db.fetchval(sql, *params)
            if val is None:
                if negative_cache and cache_key and not bypass_cache:
                    try:
                        await self._cache_set_negative(cache_key)
                    except Exception:
                        pass
                return None

            if cache_key and cache_action in ("get_or_set", "set"):
                try:
                    await self._cache_set(cache_key, {"value": _json_safe(val)}, ttl=cache_ttl)
                except Exception as exc:
                    logger.warning("Redis auto-store value failed for key %s: %s", cache_key, exc)

            return val

        return None

    # --------------------------------------------------------------------------
    # Overload Signatures for execute_write
    # --------------------------------------------------------------------------

    # 1. When schema_cls is provided and fetch_mode is "one" (default)
    @overload
    async def execute_write(
        self,
        *,
        sql: str,
        params: Sequence[Any] = (),
        fetch_mode: Literal["one"] = "one",
        schema_cls: Type[TSchema],
        to_schema: None = None,
        cache_key: Optional[str] = None,
        cache_action: Literal["delete", "set", "none"] = "delete",
        cache_value: Optional[Any] = None,
        cache_ttl: Optional[int] = None,
        extra_cache_keys: Sequence[str] = (),
        invalidate_lists: bool = True,
        es_index: Optional[str] = None,
        es_action: Literal["index", "update", "delete", "none"] = "none",
        es_id: Optional[str] = None,
        es_doc: Optional[Dict[str, Any]] = None,
        use_outbox: bool = True,
    ) -> Optional[TSchema]: ...

    # 2. When to_schema callable is provided and fetch_mode is "one"
    @overload
    async def execute_write(
        self,
        *,
        sql: str,
        params: Sequence[Any] = (),
        fetch_mode: Literal["one"] = "one",
        schema_cls: None = None,
        to_schema: Callable[[Dict[str, Any]], T],
        cache_key: Optional[str] = None,
        cache_action: Literal["delete", "set", "none"] = "delete",
        cache_value: Optional[Any] = None,
        cache_ttl: Optional[int] = None,
        extra_cache_keys: Sequence[str] = (),
        invalidate_lists: bool = True,
        es_index: Optional[str] = None,
        es_action: Literal["index", "update", "delete", "none"] = "none",
        es_id: Optional[str] = None,
        es_doc: Optional[Dict[str, Any]] = None,
        use_outbox: bool = True,
    ) -> Optional[T]: ...

    # 3. When fetch_mode is "none" (e.g. DELETE without RETURNING)
    @overload
    async def execute_write(
        self,
        *,
        sql: str,
        params: Sequence[Any] = (),
        fetch_mode: Literal["none"],
        schema_cls: Any = None,
        to_schema: Any = None,
        cache_key: Optional[str] = None,
        cache_action: Literal["delete", "set", "none"] = "delete",
        cache_value: Optional[Any] = None,
        cache_ttl: Optional[int] = None,
        extra_cache_keys: Sequence[str] = (),
        invalidate_lists: bool = True,
        es_index: Optional[str] = None,
        es_action: Literal["index", "update", "delete", "none"] = "none",
        es_id: Optional[str] = None,
        es_doc: Optional[Dict[str, Any]] = None,
        use_outbox: bool = True,
    ) -> None: ...

    # 4. Fallback Dict return when no schema is passed and fetch_mode is "one"
    @overload
    async def execute_write(
        self,
        *,
        sql: str,
        params: Sequence[Any] = (),
        fetch_mode: Literal["one"] = "one",
        schema_cls: None = None,
        to_schema: None = None,
        cache_key: Optional[str] = None,
        cache_action: Literal["delete", "set", "none"] = "delete",
        cache_value: Optional[Any] = None,
        cache_ttl: Optional[int] = None,
        extra_cache_keys: Sequence[str] = (),
        invalidate_lists: bool = True,
        es_index: Optional[str] = None,
        es_action: Literal["index", "update", "delete", "none"] = "none",
        es_id: Optional[str] = None,
        es_doc: Optional[Dict[str, Any]] = None,
        use_outbox: bool = True,
    ) -> Optional[Dict[str, Any]]: ...

    async def execute_write(
        self,
        *,
        sql: str,
        params: Sequence[Any] = (),
        fetch_mode: Literal["one", "all", "val", "none"] = "one",
        schema_cls: Optional[Type[TSchema]] = None,
        to_schema: Optional[Callable[[Dict[str, Any]], Any]] = None,
        cache_key: Optional[str] = None,
        cache_action: Literal["delete", "set", "none"] = "delete",
        cache_value: Optional[Any] = None,
        cache_ttl: Optional[int] = None,
        extra_cache_keys: Sequence[str] = (),
        invalidate_lists: bool = True,
        es_index: Optional[str] = None,
        es_action: Literal["index", "update", "delete", "none"] = "none",
        es_id: Optional[str] = None,
        es_doc: Optional[Dict[str, Any]] = None,
        use_outbox: bool = True,
    ) -> Any:
        """Executes INSERT/UPDATE/DELETE mutation with atomic Transactional Outbox, cache purge, and commit."""
        
        # 1. Execute SQL inside active transaction
        row_dict: Dict[str, Any] = {}
        if fetch_mode == "one":
            row = await self.db.fetchrow(sql, *params)
            if row is None:
                return None
            row_dict = _record_to_dict(row)
        elif fetch_mode == "none":
            status_str = await self.db.execute(sql, *params)
            if status_str in ("DELETE 0", "UPDATE 0"):
                return None
        elif fetch_mode == "val":
            val = await self.db.fetchval(sql, *params)
            if val is None:
                return None
            row_dict = {"value": val}
        elif fetch_mode == "all":
            rows = await self.db.fetch(sql, *params)
            row_dict = {"rows": [_record_to_dict(r) for r in rows]}

        json_safe_dict = _json_safe(row_dict)

        # Determine aggregate ID
        agg_id = str(es_id) if es_id is not None else str(row_dict.get("id", uuid.uuid4()))

        # 2. Stage Outbox Event BEFORE DB commit (Atomic consistency)
        outbox_event: Optional[OutboxEvent] = None
        if use_outbox and es_index and es_action != "none":
            payload_data: Dict[str, Any] = {}
            if es_action.lower() in ("index", "update", "create"):
                payload_data = es_doc if es_doc is not None else json_safe_dict
            
            outbox_event = await self.outbox_repo.create_event(
                aggregate_type=es_index,
                aggregate_id=agg_id,
                event_type=es_action.upper(),
                payload=payload_data,
            )

        # 3. Synchronize Redis Cache
        if cache_key:
            if cache_action == "delete":
                await self._cache_delete(cache_key)
            elif cache_action == "set":
                val = cache_value if cache_value is not None else json_safe_dict
                if isinstance(val, BaseModel):
                    val = val.model_dump(mode="json")
                await self._cache_set(cache_key, val, ttl=cache_ttl)

        for extra_key in extra_cache_keys:
            await self._cache_delete(extra_key)

        if invalidate_lists:
            await self.cache.invalidate_lists()

        # 4. Atomic Commit
        await self.db.commit()

        # 5. Dispatch Elasticsearch Outbox Sync
        if outbox_event is not None and es_index:
            await self._dispatch_outbox_sync(outbox_event, es_index, es_action, agg_id)

        if fetch_mode == "none":
            return None

        if fetch_mode == "val":
            return row_dict.get("value")

        return self._transform_item(row_dict, schema_cls, to_schema)

    # --------------------------------------------------------------------------
    # Overload Signatures for execute_search
    # --------------------------------------------------------------------------

    @overload
    async def execute_search(
        self,
        *,
        es_index: str,
        es_query: Union[str, Dict[str, Any]],
        limit: int = 20,
        offset: int = 0,
        schema_cls: Type[TSchema],
        to_schema: None = None,
        fallback_sql: Optional[str] = None,
        fallback_params: Sequence[Any] = (),
    ) -> List[TSchema]: ...

    @overload
    async def execute_search(
        self,
        *,
        es_index: str,
        es_query: Union[str, Dict[str, Any]],
        limit: int = 20,
        offset: int = 0,
        schema_cls: None = None,
        to_schema: Callable[[Dict[str, Any]], T],
        fallback_sql: Optional[str] = None,
        fallback_params: Sequence[Any] = (),
    ) -> List[T]: ...

    @overload
    async def execute_search(
        self,
        *,
        es_index: str,
        es_query: Union[str, Dict[str, Any]],
        limit: int = 20,
        offset: int = 0,
        schema_cls: None = None,
        to_schema: None = None,
        fallback_sql: Optional[str] = None,
        fallback_params: Sequence[Any] = (),
    ) -> List[Dict[str, Any]]: ...

    async def execute_search(
        self,
        *,
        es_index: str,
        es_query: Union[str, Dict[str, Any]],
        limit: int = 20,
        offset: int = 0,
        schema_cls: Optional[Type[TSchema]] = None,
        to_schema: Optional[Callable[[Dict[str, Any]], Any]] = None,
        fallback_sql: Optional[str] = None,
        fallback_params: Sequence[Any] = (),
    ) -> Any:
        """Executes full-text fuzzy search against Elasticsearch with graceful PostgreSQL raw SQL fallback."""
        try:
            hits, total = await self.es.search(
                index=es_index,
                query=es_query,
                limit=limit,
                offset=offset,
            )
            if hits:
                return [self._transform_item(h, schema_cls, to_schema) for h in hits]
        except Exception as exc:
            logger.warning("Elasticsearch search error (%s on %s): %s. Falling back to DB.", es_query, es_index, exc)

        # Fallback to PostgreSQL if ES fails or returns no results and fallback_sql is provided
        if fallback_sql:
            rows = await self.db.fetch(fallback_sql, *fallback_params)
            row_dicts = [_record_to_dict(r) for r in rows]
            return [self._transform_item(rd, schema_cls, to_schema) for rd in row_dicts]

        return []

    # --------------------------------------------------------------------------
    # Helper Methods
    # --------------------------------------------------------------------------
    def _transform_item(
        self,
        item: Any,
        schema_cls: Optional[Type[TSchema]],
        to_schema: Optional[Callable[[Dict[str, Any]], Any]],
    ) -> Any:
        """Transforms raw dictionary into typed schema or applies custom converter."""
        if item is None:
            return None
        if schema_cls is not None and isinstance(item, schema_cls):
            return item
        dict_val = _record_to_dict(item)
        if schema_cls is not None:
            return schema_cls.model_validate(dict_val)
        if to_schema is not None:
            return to_schema(dict_val)
        return dict_val

    async def _dispatch_outbox_sync(
        self,
        event: OutboxEvent,
        es_index: str,
        es_action: str,
        agg_id: str,
    ) -> None:
        """Dispatches outbox event synchronization to Elasticsearch."""
        if self.outbox_worker is not None:
            asyncio.create_task(self.outbox_worker.sync_now(event.id))
        else:
            try:
                action_lower = es_action.lower()
                if action_lower in ("index", "update", "create"):
                    if event.payload:
                        await self.es.index_document(index=es_index, id_=agg_id, document=event.payload)
                elif action_lower == "delete":
                    await self.es.delete_document(index=es_index, id_=agg_id)

                now = datetime.now(timezone.utc)
                await self.outbox_repo.mark_published(event.id, published_at=now)
            except Exception as exc:
                logger.warning("Direct outbox sync fallback failed for %s/%s: %s", es_index, agg_id, exc)


__all__ = [
    "QueryIntent",
    "DynamicServiceExecutor",
]
