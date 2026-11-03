"""Dynamic Redis Cluster Cache-Aside Manager.

Split out of `app/services/base.py` so this file holds exactly one
responsibility: high-performance Redis Cluster caching infrastructure.
`app/services/base.py` holds the other (the `BaseService` CRUD template
method). Neither needs to change when the other does.

- Single Item Caching: Uniform MurmurHash3 distribution across Redis Cluster shards without hash tags.
- Namespace Versioning: Instant O(1) list invalidation via monotonic counter (`INCR cache:<prefix>:list_ver`).
- Negative Caching: Short TTL caching of non-existent entity IDs to mitigate cache-penetration attacks.
- Cache Stampede Protection: Distributed mutex locking (`SET NX PX`) on cache miss.
- TTL Jitter: ±10% randomization to avoid synchronized cache expiration waves.
- Dynamic Per-Operation Feature Flags: Runtime per-call `enabled=True/False` overrides.
"""

import asyncio
import hashlib
import logging
import random
from typing import Any, Callable, Dict, Optional, Set, Union

from fastapi import Request
import orjson
from redis.asyncio import Redis, RedisCluster
from redis.exceptions import RedisError

from app.core.config import CacheOperationFlags, Settings, get_settings
from app.services.interface import ICacheService

logger = logging.getLogger(__name__)

# Sentinel object stored in Redis to represent confirmed non-existence (Negative Caching)
NEGATIVE_CACHE_SENTINEL: Dict[str, bool] = {"__not_found__": True}


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

    async def incr(self, key: str) -> int:
        """Atomically increments a counter key (`INCR`), creating it at 1 if absent.

        Used for O(1) namespace-version cache invalidation outside the
        item/list helpers above — e.g. `BaseRepository`'s dynamic per-table
        query cache bumps one of these on every write instead of tracking or
        deleting individual cached query keys.
        """
        try:
            return int(await self.redis.incr(key))
        except (RedisError, ConnectionError, OSError) as exc:
            logger.warning("Redis failure during incr(%s): %s. Failing open.", key, exc)
            return 1


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


__all__ = [
    "NEGATIVE_CACHE_SENTINEL",
    "DynamicRedisCache",
    "get_cache",
]
