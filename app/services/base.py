"""Production-Grade Generic Base Service Layer with Dynamic Redis Cache-Aside Manager.

This module unifies the generic domain orchestration layer with high-performance
Redis Cluster caching infrastructure:
1. DynamicRedisCache (ICacheService):
   - Single Item Caching: Uniform MurmurHash3 distribution across Redis Cluster shards without hash tags.
   - Namespace Versioning: Instant O(1) list invalidation via monotonic counter (`INCR cache:<prefix>:list_ver`).
   - Negative Caching: Short TTL caching of non-existent entity IDs to mitigate cache-penetration attacks.
   - Cache Stampede Protection: Distributed mutex locking (`SET NX PX`) on cache miss.
   - TTL Jitter: ±10% randomization to avoid synchronized cache expiration waves.
   - Dynamic Per-Operation Feature Flags: Runtime per-call `enabled=True/False` overrides.
2. BaseService (IBaseService):
   - Template Method Pattern: Generic CRUD workflow orchestration with transaction consistency boundaries.
   - Decoupled from transport layer: Emits domain-level exceptions (NotFoundError, ConflictError).
   - Pluggable Lifecycle Hooks: `before_create`, `before_update`, `to_read_schema`, `to_cache_dict`.
   - Keyset (cursor) B-Tree pagination and offset pagination with safety limits.
   - Polymorphic Caching: Uses NullCache transparently when no cache instance is provided.
"""

import asyncio
from collections.abc import Mapping
from datetime import datetime, timezone
import hashlib
import logging
import random
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Sequence,
    Set,
    Tuple,
    Type,
    TypeVar,
    Union,
)
import uuid

from fastapi import Request
import orjson
from pydantic import BaseModel
from redis.asyncio import Redis, RedisCluster
from redis.exceptions import RedisError
from sqlalchemy import Uuid
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import CacheOperationFlags, Settings, get_settings
from app.core.elasticsearch import IElasticsearchService, NullElasticsearch
from app.core.exceptions import ConflictError, NotFoundError
from app.models.base import Base
from app.models.outbox import OutboxEvent
from app.repositories.interface import IBaseRepository
from app.schemas.base import decode_cursor, encode_cursor
from app.services.interface import IBaseService, ICacheService, NullCache

logger = logging.getLogger(__name__)

# Sentinel object stored in Redis to represent confirmed non-existence (Negative Caching)
NEGATIVE_CACHE_SENTINEL: Dict[str, bool] = {"__not_found__": True}


# ==============================================================================
# Dynamic Redis Cluster Cache-Aside Manager
# ==============================================================================


class DynamicRedisCache(ICacheService):
    """Production-grade Redis Cluster Cache-Aside manager with full dynamic per-call control."""

    def __init__(
        self,
        redis: Union[RedisCluster, Redis],
        prefix: str,
        item_ttl: int = 300,
        list_ttl: int = 120,
        negative_ttl: int = 30,
        lock_ttl_ms: int = 5000,
        flags: Optional[CacheOperationFlags] = None,
        allowed_filter_keys: Optional[Set[str]] = None,
    ) -> None:
        self.redis: Union[RedisCluster, Redis] = redis
        self.prefix: str = prefix
        self.item_ttl: int = item_ttl
        self.list_ttl: int = list_ttl
        self.negative_ttl: int = negative_ttl
        self.lock_ttl_ms: int = lock_ttl_ms
        self.flags: CacheOperationFlags = flags or CacheOperationFlags()
        self._allowed_filter_keys: Optional[Set[str]] = allowed_filter_keys

    def _is_op_enabled(self, default_flag: bool, override: Optional[bool]) -> bool:
        """Determines if operation is enabled: per-call override takes precedence over global flag."""
        return default_flag if override is None else override

    def _jitter(self, base_ttl: int) -> int:
        """Applies ±10% jitter to prevent synchronized cache expiration."""
        spread: float = base_ttl * 0.10
        jittered: float = base_ttl + random.uniform(-spread, spread)
        return max(1, int(jittered))

    def _item_key(self, item_id: Any) -> str:
        """Single item key format without hash tags to spread across cluster shards."""
        return f"cache:{self.prefix}:item:{item_id}"

    def _list_version_key(self) -> str:
        """Key holding the list namespace version counter."""
        return f"cache:{self.prefix}:list_ver"

    def _lock_key(self, item_id: Any) -> str:
        """Distributed lock key for stampede prevention."""
        return f"cache:{self.prefix}:lock:{item_id}"

    def _hash_filters(self, **filters: Any) -> str:
        """Normalizes and deterministically hashes query parameters dynamically."""
        normalized: Dict[str, str] = {}
        for k, v in filters.items():
            if v is None or k.startswith("_"):
                continue
            # If allowed whitelist is configured, check membership; otherwise include all non-None
            if self._allowed_filter_keys is None or k in self._allowed_filter_keys:
                normalized[k] = str(v)

        if not normalized:
            return "all"

        # Deterministic canonical serialization (sorted keys, compact separators)
        serialized: str = orjson.dumps(normalized, option=orjson.OPT_SORT_KEYS).decode("utf-8")
        return hashlib.sha256(serialized.encode("utf-8")).hexdigest()[:16]

    # --------------------------------------------------------------------------
    # Single Item Cache-Aside Operations
    # --------------------------------------------------------------------------

    async def get_item(self, item_id: Any, enabled: Optional[bool] = None) -> Optional[Dict[str, Any]]:
        """Retrieves single item from cache if enabled (globally or via per-call override).

        Returns:
            - Dict if found
            - NEGATIVE_CACHE_SENTINEL if negatively cached (not found)
            - None if cache miss or operation disabled/failed
        """
        if not self._is_op_enabled(self.flags.enable_get_item, enabled):
            return None

        key: str = self._item_key(item_id)
        try:
            raw: Optional[bytes] = await self.redis.get(key)
            if raw is None:
                return None
            return orjson.loads(raw)
        except (RedisError, ConnectionError, OSError) as exc:
            logger.warning("Redis failure during get_item (%s): %s. Failing open.", key, exc)
            return None

    async def set_item(self, item_id: Any, data: Dict[str, Any], enabled: Optional[bool] = None) -> bool:
        """Serializes and stores item in cache with TTL jitter if enabled."""
        if not self._is_op_enabled(self.flags.enable_set_item, enabled):
            return False

        key: str = self._item_key(item_id)
        ttl: int = self._jitter(self.item_ttl)
        try:
            payload: bytes = orjson.dumps(data)
            await self.redis.set(key, payload, ex=ttl)
            return True
        except (RedisError, ConnectionError, OSError) as exc:
            logger.warning("Redis failure during set_item (%s): %s", key, exc)
            return False

    async def set_negative_cache(self, item_id: Any, enabled: Optional[bool] = None) -> bool:
        """Caches negative lookup result (not found) for a brief period."""
        if not self._is_op_enabled(self.flags.enable_negative_cache, enabled) or not self._is_op_enabled(
            self.flags.enable_set_item, enabled
        ):
            return False

        key: str = self._item_key(item_id)
        ttl: int = self._jitter(self.negative_ttl)
        try:
            payload: bytes = orjson.dumps(NEGATIVE_CACHE_SENTINEL)
            await self.redis.set(key, payload, ex=ttl)
            return True
        except (RedisError, ConnectionError, OSError) as exc:
            logger.warning("Redis failure during set_negative_cache (%s): %s", key, exc)
            return False

    async def delete_item(self, item_id: Any, enabled: Optional[bool] = None) -> bool:
        """Deletes item from cache (used during update/delete writes)."""
        if not self._is_op_enabled(self.flags.enable_delete_item, enabled):
            return False

        key: str = self._item_key(item_id)
        try:
            await self.redis.delete(key)
            return True
        except (RedisError, ConnectionError, OSError) as exc:
            logger.warning("Redis failure during delete_item (%s): %s", key, exc)
            return False

    # --------------------------------------------------------------------------
    # List / Paginated Cache-Aside with Namespace Versioning
    # --------------------------------------------------------------------------

    async def get_list_version(self) -> int:
        """Gets current list version number for namespace invalidation."""
        try:
            ver: Optional[bytes] = await self.redis.get(self._list_version_key())
            return int(ver) if ver else 1
        except (RedisError, ConnectionError, OSError):
            return 1

    async def get_paginated(
        self,
        enabled: Optional[bool] = None,
        **filters: Any,
    ) -> Optional[Dict[str, Any]]:
        """Retrieves paginated list under current namespace version."""
        if not self._is_op_enabled(self.flags.enable_get_list, enabled):
            return None

        try:
            version: int = await self.get_list_version()
            f_hash: str = self._hash_filters(**filters)
            key: str = f"cache:{self.prefix}:list:v{version}:{f_hash}"
            raw: Optional[bytes] = await self.redis.get(key)
            if raw is None:
                return None
            return orjson.loads(raw)
        except (RedisError, ConnectionError, OSError) as exc:
            logger.warning("Redis failure during get_paginated: %s. Failing open.", exc)
            return None

    async def set_paginated(
        self,
        data: Dict[str, Any],
        enabled: Optional[bool] = None,
        **filters: Any,
    ) -> bool:
        """Caches paginated results with current namespace version and jittered TTL."""
        if not self._is_op_enabled(self.flags.enable_set_list, enabled):
            return False

        try:
            version: int = await self.get_list_version()
            f_hash: str = self._hash_filters(**filters)
            key: str = f"cache:{self.prefix}:list:v{version}:{f_hash}"
            ttl: int = self._jitter(self.list_ttl)
            payload: bytes = orjson.dumps(data)
            await self.redis.set(key, payload, ex=ttl)
            return True
        except (RedisError, ConnectionError, OSError) as exc:
            logger.warning("Redis failure during set_paginated: %s", exc)
            return False

    async def invalidate_lists(self, enabled: Optional[bool] = None) -> bool:
        """Invalidates all list cache keys instantly via atomic version increment (O(1))."""
        if not self._is_op_enabled(self.flags.enable_invalidate_list, enabled):
            return False

        try:
            await self.redis.incr(self._list_version_key())
            return True
        except (RedisError, ConnectionError, OSError) as exc:
            logger.warning("Redis failure during invalidate_lists: %s", exc)
            return False

    # --------------------------------------------------------------------------
    # Stampede Protection (Mutex Locking)
    # --------------------------------------------------------------------------

    async def get_or_compute_item(
        self,
        item_id: Any,
        compute_fn: Callable[[], Any],
        enabled: Optional[bool] = None,
        lock_enabled: Optional[bool] = None,
    ) -> Optional[Dict[str, Any]]:
        """Executes cache-aside read with stampede protection (SET NX PX lock).

        Supports runtime per-call enable/disable overrides for both caching and locking.
        """
        # Fast path if cache disabled
        if not self._is_op_enabled(self.flags.enable_get_item, enabled):
            return await compute_fn()

        cached: Optional[Dict[str, Any]] = await self.get_item(item_id, enabled=enabled)
        if cached is not None:
            return cached

        # Stampede lock disabled -> direct compute and set
        if not self._is_op_enabled(self.flags.enable_stampede_lock, lock_enabled):
            computed: Optional[Dict[str, Any]] = await compute_fn()
            if computed is not None:
                await self.set_item(item_id, computed, enabled=enabled)
            else:
                await self.set_negative_cache(item_id, enabled=enabled)
            return computed

        lock_key: str = self._lock_key(item_id)
        acquired: bool = False
        try:
            acquired = bool(await self.redis.set(lock_key, "1", nx=True, px=self.lock_ttl_ms))
        except (RedisError, ConnectionError, OSError):
            acquired = False

        if acquired:
            try:
                computed_val: Optional[Dict[str, Any]] = await compute_fn()
                if computed_val is not None:
                    await self.set_item(item_id, computed_val, enabled=enabled)
                else:
                    await self.set_negative_cache(item_id, enabled=enabled)
                return computed_val
            finally:
                try:
                    await self.redis.delete(lock_key)
                except Exception:
                    pass
        else:
            # Another process is computing: back off briefly and re-fetch from cache
            await asyncio.sleep(0.05)
            second_try: Optional[Dict[str, Any]] = await self.get_item(item_id, enabled=enabled)
            if second_try is not None:
                return second_try
            # If still not populated, fallback to direct compute
            return await compute_fn()

    # --------------------------------------------------------------------------
    # Core Key-Value Operations
    # --------------------------------------------------------------------------

    async def get(self, key: str) -> Optional[Any]:
        """Retrieves and deserializes JSON value by raw key."""
        try:
            raw = await self.redis.get(key)
            if raw is None:
                return None
            return orjson.loads(raw)
        except Exception as exc:
            logger.warning("Redis failure during get(%s): %s", key, exc)
            return None

    async def set(self, key: str, value: Any, ttl: Optional[int] = None) -> bool:
        """Serializes and stores value under raw key with optional TTL."""
        try:
            payload = orjson.dumps(value)
            if ttl is not None:
                await self.redis.set(key, payload, ex=ttl)
            else:
                await self.redis.set(key, payload)
            return True
        except Exception as exc:
            logger.warning("Redis failure during set(%s): %s", key, exc)
            return False

    async def delete(self, key: str) -> bool:
        """Deletes raw key from cache."""
        try:
            res = await self.redis.delete(key)
            return bool(res)
        except Exception as exc:
            logger.warning("Redis failure during delete(%s): %s", key, exc)
            return False

    async def exists(self, key: str) -> bool:
        """Checks if key exists in cache."""
        try:
            res = await self.redis.exists(key)
            return bool(res)
        except Exception as exc:
            logger.warning("Redis failure during exists(%s): %s", key, exc)
            return False

    async def expire(self, key: str, ttl: int) -> bool:
        """Sets TTL on raw key."""
        try:
            res = await self.redis.expire(key, ttl)
            return bool(res)
        except Exception as exc:
            logger.warning("Redis failure during expire(%s): %s", key, exc)
            return False

    async def ttl(self, key: str) -> int:
        """Returns remaining TTL on key in seconds (-2 if not found, -1 if no TTL)."""
        try:
            res = await self.redis.ttl(key)
            return int(res)
        except Exception as exc:
            logger.warning("Redis failure during ttl(%s): %s", key, exc)
            return -2


def get_cache(
    prefix: str,
    allowed_filter_keys: Optional[Set[str]] = None,
) -> Callable[[Request], DynamicRedisCache]:
    """FastAPI dependency factory returning a DynamicRedisCache bound to request's Redis client."""

    def _cache_dependency(request: Request, settings: Settings = get_settings()) -> DynamicRedisCache:
        redis_client: Union[RedisCluster, Redis] = request.app.state.redis
        flags: CacheOperationFlags = settings.get_cache_flags()
        return DynamicRedisCache(
            redis=redis_client,
            prefix=prefix,
            item_ttl=settings.CACHE_ITEM_TTL,
            list_ttl=settings.CACHE_LIST_TTL,
            negative_ttl=settings.CACHE_NEGATIVE_TTL,
            lock_ttl_ms=settings.CACHE_LOCK_TTL_MS,
            flags=flags,
            allowed_filter_keys=allowed_filter_keys,
        )

    return _cache_dependency


# ==============================================================================
# Generic Base Service Layer (Template Method Pattern)
# ==============================================================================

ModelType = TypeVar("ModelType", bound=Base)
CreateSchemaType = TypeVar("CreateSchemaType", bound=BaseModel)
UpdateSchemaType = TypeVar("UpdateSchemaType", bound=BaseModel)
ReadSchemaType = TypeVar("ReadSchemaType", bound=BaseModel)
IdType = TypeVar("IdType")


def format_event_type(
    *args: str,
    service: Optional[str] = None,
    resource: Optional[str] = None,
    model: Optional[str] = None,
    action: Optional[str] = None,
    version: Optional[str] = None,
) -> str:
    """Format and validate a standardized event_type conforming to:
    '<service_name>.<model_name>.<action_name>[.<version>]'

    Convention:
    - service_name: Defaults to settings.APP_NAME (e.g. 'HighPerfStatelessService').
    - model_name: Domain entity/model name, normalized to lowercase (e.g. 'user', 'category').
    - action_name: Domain action, normalized to lowercase (e.g. 'created', 'updated', 'deleted').
    - version: Optional version tag (e.g. 'v1'). Appended only if explicitly specified.

    Examples:
        format_event_type("user", "created") -> "HighPerfStatelessService.user.created"
        format_event_type("user", "created", "v1") -> "HighPerfStatelessService.user.created.v1"
        format_event_type("user", "created", version="v1") -> "HighPerfStatelessService.user.created.v1"
        format_event_type("HighPerfStatelessService", "user", "created") -> "HighPerfStatelessService.user.created"
        format_event_type("AuthService", "user", "created") -> "AuthService.user.created"
    """
    srv: Optional[str] = service
    res: Optional[str] = model or resource
    act: Optional[str] = action
    ver: Optional[str] = version

    if len(args) == 1:
        res = args[0]
    elif len(args) == 2:
        res, act = args[0], args[1]
    elif len(args) == 3:
        arg2_clean = str(args[2]).strip().lower()
        is_version = arg2_clean.isdigit() or (
            arg2_clean.startswith("v")
            and len(arg2_clean) > 1
            and (arg2_clean[1:].isdigit() or "." in arg2_clean)
        )
        if is_version:
            res, act, ver = args[0], args[1], args[2]
        else:
            srv, res, act = args[0], args[1], args[2]
    elif len(args) >= 4:
        srv, res, act, ver = args[0], args[1], args[2], args[3]

    if srv is None:
        try:
            from app.core.config import get_settings
            srv = get_settings().APP_NAME
        except Exception:
            srv = os.getenv("APP_NAME", "HighPerfStatelessService")

    clean_srv = str(srv).strip() if srv else ""
    clean_res = str(res or "").strip().lower()
    clean_act = str(act or "").strip().lower()

    parts: list[str] = []
    if clean_srv:
        parts.append(clean_srv)
    if clean_res:
        parts.append(clean_res)
    if clean_act:
        parts.append(clean_act)

    if ver is not None:
        clean_ver = str(ver).strip().lower()
        if clean_ver.isdigit():
            clean_ver = f"v{clean_ver}"
        if clean_ver:
            parts.append(clean_ver)

    return ".".join(parts)


class BaseService(IBaseService[ModelType, CreateSchemaType, UpdateSchemaType, ReadSchemaType, IdType]):
    """Generic domain service orchestrator implementing IBaseService with polymorphic caching and Elasticsearch search offloading."""

    def __init__(
        self,
        repo: IBaseRepository[ModelType, IdType],
        read_schema_cls: Type[ReadSchemaType],
        cache: Optional[ICacheService] = None,
        elasticsearch: Optional[IElasticsearchService] = None,
        order_col: str = "created_at",
        id_col: str = "id",
        db: Optional[AsyncSession] = None,
        session: Optional[AsyncSession] = None,
        outbox_enabled: bool = False,
        auto_outbox_events: bool = False,
        outbox_topic: str = "domain.events",
        service_name: Optional[str] = None,
    ) -> None:
        self.repo: IBaseRepository[ModelType, IdType] = repo
        self.repository: IBaseRepository[ModelType, IdType] = repo
        active_db = db or session or getattr(repo, "_session", None) or getattr(repo, "session", None) or getattr(repo, "db", None)
        self.db: Optional[AsyncSession] = active_db
        self.session: Optional[AsyncSession] = active_db
        self.read_schema_cls: Type[ReadSchemaType] = read_schema_cls
        # Polymorphic caching: uses NullCache when no distributed cache is supplied
        self.cache: ICacheService = cache if cache is not None else NullCache()
        # Elasticsearch CQRS search engine: uses NullElasticsearch when disabled/absent
        self.elasticsearch: IElasticsearchService = (
            elasticsearch if elasticsearch is not None else NullElasticsearch()
        )
        self.es: IElasticsearchService = self.elasticsearch
        self.order_col: str = order_col
        self.id_col: str = id_col
        self.outbox_enabled = outbox_enabled
        self.auto_outbox_events = auto_outbox_events
        self.outbox_topic = outbox_topic
        self.service_name = service_name or get_settings().APP_NAME

    @property
    def resource_name(self) -> str:
        """Returns the domain entity resource name."""
        return self.repo.model.__name__

    # --------------------------------------------------------------------------
    # Pluggable Domain Lifecycle Hooks
    # --------------------------------------------------------------------------

    async def before_create(self, payload: CreateSchemaType) -> ModelType:
        """Hook called before inserting entity into repository.

        Default implementation instantiates model directly from payload dictionary.
        Override to implement custom ID generation, encryption, or blind indexing.
        """
        data: Dict[str, Any] = payload.model_dump()
        entity: ModelType = self.repo.model(**data)
        id_column = self.repo.model.__table__.c.get(self.id_col)
        if (
            id_column is not None
            and isinstance(id_column.type, Uuid)
            and getattr(entity, self.id_col, None) is None
        ):
            setattr(entity, self.id_col, uuid.uuid4())
        if hasattr(entity, "created_at") and getattr(entity, "created_at", None) is None:
            setattr(entity, "created_at", datetime.now(timezone.utc))
        return entity

    async def before_update(self, entity: ModelType, payload: UpdateSchemaType) -> None:
        """Hook called before updating entity.

        Uses model_dump(exclude_unset=True) to support partial updates safely.
        """
        update_data: Dict[str, Any] = payload.model_dump(exclude_unset=True)
        for key, val in update_data.items():
            if hasattr(entity, key) and val is not None:
                setattr(entity, key, val)

    async def before_delete(self, entity: ModelType) -> None:
        """Hook called before deleting entity. Override to enforce custom cascading rules."""
        pass

    async def to_read_schema(self, entity: ModelType) -> ReadSchemaType:
        """Hook converting database model to public read schema.

        Override to decrypt ciphertext or mask PII fields before returning.
        """
        if hasattr(entity, "created_at") and getattr(entity, "created_at", None) is None:
            setattr(entity, "created_at", datetime.now(timezone.utc))
        return self.read_schema_cls.model_validate(entity)

    async def to_cache_dict(self, entity: ModelType) -> Dict[str, Any]:
        """Hook serializing database model to safe cached representation.

        Defaults to the JSON representation of the read schema.
        """
        read_model: ReadSchemaType = await self.to_read_schema(entity)
        return read_model.model_dump(mode="json")

    def build_event_type(
        self,
        action: str,
        resource: Optional[str] = None,
        model: Optional[str] = None,
        service: Optional[str] = None,
        version: Optional[str] = None,
    ) -> str:
        """Construct the standardized event_type following the '<service_name>.<model_name>.<action_name>[.<version>]' convention.

        Pulls the service name from `self.service_name` (defaults to settings.APP_NAME)
        and the model name from `self.resource_name.lower()`.
        """
        mod = model or resource or self.resource_name
        srv = service or getattr(self, "service_name", None)
        return format_event_type(model=mod, action=action, service=srv, version=version)

    def enqueue_outbox_event(
        self,
        action: str,
        entity: ModelType,
        *,
        event_type: Optional[str] = None,
        model: Optional[str] = None,
        resource: Optional[str] = None,
        service: Optional[str] = None,
        version: Optional[str] = None,
        extra_data: Optional[Dict[str, Any]] = None,
        force: bool = False,
    ) -> Optional[OutboxEvent]:
        """Explicitly enqueue an outbox event.

        Allows developers to emit domain events on demand when needed,
        without relying on automatic dynamic triggers from generic CRUD operations.

        Convention: '<service_name>.<model_name>.<action_name>[.<version>]'
        (e.g., 'HighPerfStatelessService.user.created').
        """
        if not self.outbox_enabled and not force:
            return None
        return self._enqueue_outbox_event(
            action=action,
            entity=entity,
            event_type=event_type,
            model=model or resource,
            service=service,
            version=version,
            extra_data=extra_data,
        )

    def _enqueue_outbox_event(
        self,
        action: str,
        entity: ModelType,
        event_type: Optional[str] = None,
        extra_data: Optional[Dict[str, Any]] = None,
        model: Optional[str] = None,
        resource: Optional[str] = None,
        service: Optional[str] = None,
        version: Optional[str] = None,
    ) -> Optional[OutboxEvent]:
        """Adds a non-sensitive event envelope to the current DB transaction.

        Uses `build_event_type` to guarantee the standardized event_type convention:
        '<service_name>.<model_name>.<action_name>[.<version>]' (e.g., 'HighPerfStatelessService.user.created').
        """
        if not self.outbox_enabled:
            return None
        if self.db is None:
            raise RuntimeError("Kafka outbox events require an active database session.")

        entity_id = str(getattr(entity, self.id_col))
        event_id = uuid.uuid4()
        resolved_event_type = event_type or self.build_event_type(
            action=action,
            model=model or resource,
            service=service,
            version=version,
        )
        outbox_event = OutboxEvent(
            id=event_id,
            topic=self.outbox_topic,
            event_key=entity_id,
            event_type=resolved_event_type,
            payload={
                "event_id": str(event_id),
                "event_type": resolved_event_type,
                "aggregate_type": self.resource_name,
                "aggregate_id": entity_id,
                "occurred_at": datetime.now(timezone.utc).isoformat(),
                "data": {"id": entity_id, **(extra_data or {})},
            },
        )
        self.db.add(outbox_event)
        return outbox_event

    # --------------------------------------------------------------------------
    # Generic CRUD Operations
    # --------------------------------------------------------------------------

    async def create(
        self,
        payload: CreateSchemaType,
        cache_enabled: Optional[bool] = None,
        emit_outbox_event: Optional[bool] = None,
    ) -> ReadSchemaType:
        """Creates entity, invalidates list cache, commits transaction, syncs to ES, and returns read schema."""
        entity: ModelType = await self.before_create(payload)
        try:
            await self.repo.create(entity)
            if self.outbox_enabled and (
                emit_outbox_event is True
                or (emit_outbox_event is None and self.auto_outbox_events is True)
            ):
                self._enqueue_outbox_event("created", entity)
            await self.cache.invalidate_lists(enabled=cache_enabled)
            if self.db is not None:
                await self.db.commit()

            # CQRS: sync created document to Elasticsearch for text search offloading
            try:
                doc = await self.to_cache_dict(entity)
                entity_id = getattr(entity, self.id_col, "")
                await self.elasticsearch.index_document(
                    index=self.resource_name.lower(),
                    id_=str(entity_id),
                    document=doc,
                )
            except Exception as es_err:
                logger.warning("Elasticsearch index failed on create (%s): %s", self.resource_name, es_err)

            return await self.to_read_schema(entity)
        except IntegrityError as exc:
            if self.db is not None:
                await self.db.rollback()
            raise ConflictError("Conflict: duplicate unique constraint violation.") from exc

    async def get(
        self,
        entity_id: IdType,
        cache_enabled: Optional[bool] = None,
    ) -> Optional[ReadSchemaType]:
        """Retrieves entity by ID returning None if not found."""
        try:
            return await self.get_by_id(entity_id, cache_enabled=cache_enabled)
        except NotFoundError:
            return None

    async def get_by_id(
        self,
        entity_id: IdType,
        cache_enabled: Optional[bool] = None,
    ) -> ReadSchemaType:
        """Retrieves entity by ID using polymorphic cache-aside with stampede protection."""
        # 1. Check item cache (NullCache immediately returns None)
        cached_data: Optional[Dict[str, Any]] = await self.cache.get_item(entity_id, enabled=cache_enabled)
        if cached_data is not None:
            if cached_data == NEGATIVE_CACHE_SENTINEL:
                raise NotFoundError(f"{self.repo.model.__name__} with ID '{entity_id}' not found")
            return self.read_schema_cls(**cached_data)

        # 2. Cache stampede-protected fetch on cache miss (NullCache executes _fetch directly)
        async def _fetch() -> Optional[Dict[str, Any]]:
            entity: Optional[ModelType] = await self.repo.get(entity_id)
            if entity is None:
                return None
            return await self.to_cache_dict(entity)

        data: Optional[Dict[str, Any]] = await self.cache.get_or_compute_item(entity_id, _fetch, enabled=cache_enabled)
        if data is None:
            raise NotFoundError(f"{self.repo.model.__name__} with ID '{entity_id}' not found")

        return self.read_schema_cls(**data)

    async def get_all(
        self,
        limit: int = 100,
        offset: int = 0,
        **filters: Any,
    ) -> List[ReadSchemaType]:
        """Retrieves all entities matching filters with limit and offset."""
        if hasattr(self.repo, "get_all"):
            entities = await self.repo.get_all(limit=limit, offset=offset, **filters)
            return [await self.to_read_schema(e) for e in entities]
        return await self.list_offset(offset=offset, limit=limit, **filters)

    async def update(
        self,
        entity_id: IdType,
        payload: UpdateSchemaType,
        cache_enabled: Optional[bool] = None,
        emit_outbox_event: Optional[bool] = None,
    ) -> Optional[ReadSchemaType]:
        """Updates entity, commits transaction, and returns updated read schema or None if not found."""
        entity: Optional[ModelType] = await self.repo.get(entity_id)
        if entity is None:
            return None

        try:
            await self.before_update(entity, payload)
            if hasattr(self.repo, "update"):
                await self.repo.update(entity)
            else:
                await self.repo.flush()
            if self.outbox_enabled and (
                emit_outbox_event is True
                or (emit_outbox_event is None and self.auto_outbox_events is True)
            ):
                self._enqueue_outbox_event("updated", entity)

            # Invalidate item and list caches polymorphically
            await self.cache.delete_item(entity_id, enabled=cache_enabled)
            await self.cache.invalidate_lists(enabled=cache_enabled)

            if self.db is not None:
                await self.db.commit()

            # CQRS: update document in Elasticsearch
            try:
                doc = await self.to_cache_dict(entity)
                await self.elasticsearch.update_document(
                    index=self.resource_name.lower(),
                    id_=str(entity_id),
                    document=doc,
                )
            except Exception as es_err:
                logger.warning("Elasticsearch index failed on update (%s): %s", self.resource_name, es_err)

            return await self.to_read_schema(entity)
        except IntegrityError as exc:
            if self.db is not None:
                await self.db.rollback()
            raise ConflictError("Conflict: update violates unique database constraint.") from exc

    async def update_by_id(
        self,
        entity_id: IdType,
        payload: UpdateSchemaType,
        cache_enabled: Optional[bool] = None,
        emit_outbox_event: Optional[bool] = None,
    ) -> ReadSchemaType:
        """Updates entity by ID, raising NotFoundError if entity does not exist."""
        updated = await self.update(
            entity_id,
            payload,
            cache_enabled=cache_enabled,
            emit_outbox_event=emit_outbox_event,
        )
        if updated is None:
            raise NotFoundError(f"{self.repo.model.__name__} with ID '{entity_id}' not found")
        return updated

    async def delete(
        self,
        entity_id: IdType,
        cache_enabled: Optional[bool] = None,
        emit_outbox_event: Optional[bool] = None,
    ) -> bool:
        """Deletes entity and invalidates item and list caches. Returns True if deleted, False if not found."""
        entity: Optional[ModelType] = await self.repo.get(entity_id)
        if entity is None:
            return False

        await self.before_delete(entity)
        await self.repo.delete(entity)
        if self.outbox_enabled and (
            emit_outbox_event is True
            or (emit_outbox_event is None and self.auto_outbox_events is True)
        ):
            self._enqueue_outbox_event("deleted", entity)

        # Invalidate item and list caches polymorphically
        await self.cache.delete_item(entity_id, enabled=cache_enabled)
        await self.cache.invalidate_lists(enabled=cache_enabled)

        if self.db is not None:
            await self.db.commit()

        # CQRS: remove document from Elasticsearch
        try:
            await self.elasticsearch.delete_document(
                index=self.resource_name.lower(),
                id_=str(entity_id),
            )
        except Exception as es_err:
            logger.warning("Elasticsearch delete failed on delete (%s): %s", self.resource_name, es_err)

        return True

    async def delete_by_id(
        self,
        entity_id: IdType,
        cache_enabled: Optional[bool] = None,
        emit_outbox_event: Optional[bool] = None,
    ) -> None:
        """Deletes entity by ID, raising NotFoundError if entity not found."""
        deleted = await self.delete(
            entity_id,
            cache_enabled=cache_enabled,
            emit_outbox_event=emit_outbox_event,
        )
        if not deleted:
            raise NotFoundError(f"{self.repo.model.__name__} with ID '{entity_id}' not found")

    async def list_keyset(
        self,
        limit: int = 50,
        cursor: Optional[str] = None,
        cache_enabled: Optional[bool] = None,
        **filters: Any,
    ) -> Dict[str, Any]:
        """Dynamic Keyset (cursor) pagination with polymorphic namespace-versioned list caching."""
        safe_limit: int = min(100, max(1, limit))
        filter_params: Dict[str, Any] = {"limit": safe_limit, "cursor": cursor, **filters}

        # 1. Check list cache (NullCache immediately returns None)
        cached_page: Optional[Dict[str, Any]] = await self.cache.get_paginated(enabled=cache_enabled, **filter_params)
        if cached_page is not None:
            return cached_page

        # 2. Decode keyset cursor
        cursor_dt: Optional[datetime] = None
        cursor_id: Optional[Any] = None
        if cursor:
            cursor_dt, cursor_id = decode_cursor(cursor)

        # 3. Query repository dynamically
        items_db: List[ModelType]
        has_next: bool
        items_db, has_next = await self.repo.list_keyset(
            limit=safe_limit,
            cursor_dt=cursor_dt,
            cursor_id=cursor_id,
            order_col=self.order_col,
            id_col=self.id_col,
            **filters,
        )

        # 4. Transform entities using domain hook
        items_schema: List[ReadSchemaType] = [await self.to_read_schema(u) for u in items_db]

        # 5. Build next cursor if additional page exists
        next_cursor: Optional[str] = None
        if has_next and items_db:
            last_item: ModelType = items_db[-1]
            last_dt: Any = getattr(last_item, self.order_col)
            last_id: Any = getattr(last_item, self.id_col)
            next_cursor = encode_cursor(last_dt, last_id)

        page_result: Dict[str, Any] = {
            "items": [item.model_dump(mode="json") for item in items_schema],
            "next_cursor": next_cursor,
            "limit": safe_limit,
        }

        # 6. Cache result polymorphically
        await self.cache.set_paginated(page_result, enabled=cache_enabled, **filter_params)

        return page_result

    async def list_offset(
        self,
        offset: int = 0,
        limit: int = 50,
        order_by: Optional[str] = None,
        descending: bool = False,
        **filters: Any,
    ) -> List[ReadSchemaType]:
        """Standard offset-based listing for smaller datasets or admin screens."""
        safe_limit: int = min(100, max(1, limit))
        entities: Sequence[ModelType] = await self.repo.list(
            offset=offset,
            limit=safe_limit,
            order_by=order_by,
            descending=descending,
            **filters,
        )
        return [await self.to_read_schema(e) for e in entities]

    async def list(
        self,
        *,
        filters: Optional[Mapping[str, Any]] = None,
        sort: Optional[Sequence[str]] = None,
        page: int = 1,
        size: int = 20,
    ) -> Tuple[Sequence[ReadSchemaType], int]:
        """Paginated listing returning (items, total) to power CRUDRouter and Page schemas."""
        safe_size: int = min(100, max(1, size))
        safe_page: int = max(1, page)
        total = await self.repo.count(filters=filters)
        entities = await self.repo.list(
            filters=filters,
            sort=sort,
            offset=(safe_page - 1) * safe_size,
            limit=safe_size,
        )
        items = [await self.to_read_schema(e) for e in entities]
        return items, total

    async def search(
        self,
        query: Union[str, Dict[str, Any]],
        limit: int = 20,
        offset: int = 0,
    ) -> Tuple[List[Dict[str, Any]], int]:
        """Full-text search offloaded directly to Elasticsearch (CQRS)."""
        return await self.elasticsearch.search(
            index=self.resource_name.lower(),
            query=query,
            limit=limit,
            offset=offset,
        )


__all__ = [
    "NEGATIVE_CACHE_SENTINEL",
    "DynamicRedisCache",
    "get_cache",
    "BaseService",
    "format_event_type",
]
