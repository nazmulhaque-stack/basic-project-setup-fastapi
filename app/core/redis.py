"""Centralized asynchronous Redis infrastructure and lifecycle management.

Provides connection pooling, standalone and cluster support, health checks,
temporary state helpers (idempotency, challenges, distributed locks), and cache primitives.
"""

from collections.abc import Callable
import functools
import hashlib
import logging
import os
import random
import time
from typing import Any, Dict, List, Optional, Tuple, Union

import orjson
from redis.asyncio import ConnectionPool, Redis, RedisCluster
from redis.asyncio.cluster import ClusterNode
from redis.exceptions import AuthenticationError, ConnectionError as RedisConnError, RedisClusterException, RedisError, TimeoutError as RedisTimeoutError

from app.core.config import Settings, get_settings

logger = logging.getLogger(__name__)

# Atomic Distributed Lock Release Script (releases lock ONLY if token matches owner)
RELEASE_LOCK_LUA = """
if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("del", KEYS[1])
else
    return 0
end
"""


# ==============================================================================
# Centralized Client Factory & Lifecycle
# ==============================================================================


async def create_redis_client(
    settings: Optional[Settings] = None,
) -> Union[RedisCluster, Redis]:
    """Initializes and verifies an asynchronous Redis connection.

    Prioritizes REDIS_URL for standalone instances, or parses REDIS_CLUSTER_NODES
    for Redis 7 Cluster. Automatically falls back to standalone if cluster mode is inactive.
    """
    cfg = settings or get_settings()

    # 1. Standalone Redis via REDIS_URL
    if cfg.REDIS_URL:
        logger.info("Initializing standalone Redis client from REDIS_URL")
        pool = ConnectionPool.from_url(
            cfg.REDIS_URL,
            max_connections=cfg.REDIS_MAX_CONNECTIONS,
            socket_timeout=cfg.REDIS_SOCKET_TIMEOUT,
            socket_connect_timeout=cfg.REDIS_CONNECT_TIMEOUT,
            decode_responses=False,
        )
        client = Redis(connection_pool=pool)
        await client.ping()
        logger.info("Connected to standalone Redis at %s", cfg.REDIS_URL)
        return client

    # 2. Redis 7 Cluster via REDIS_CLUSTER_NODES
    startup_nodes = [ClusterNode(host=h, port=p) for h, p in cfg.parsed_redis_nodes]
    redis_password = cfg.REDIS_PASSWORD or None
    try:
        cluster_client = RedisCluster(
            startup_nodes=startup_nodes,
            password=redis_password,
            socket_timeout=cfg.REDIS_SOCKET_TIMEOUT,
            socket_connect_timeout=cfg.REDIS_CONNECT_TIMEOUT,
            decode_responses=False,
        )
        await cluster_client.ping()
        logger.info("Connected to Redis 7 Cluster with %d startup nodes", len(startup_nodes))
        return cluster_client
    except AuthenticationError as auth_err:
        if redis_password and "without any password configured" in str(auth_err).lower():
            logger.warning("Redis Cluster has no password configured; retrying connection without password.")
            cluster_client = RedisCluster(
                startup_nodes=startup_nodes,
                password=None,
                socket_timeout=cfg.REDIS_SOCKET_TIMEOUT,
                socket_connect_timeout=cfg.REDIS_CONNECT_TIMEOUT,
                decode_responses=False,
            )
            await cluster_client.ping()
            logger.info("Connected to Redis 7 Cluster without authentication")
            return cluster_client
        raise
    except (RedisClusterException, RedisError, OSError) as exc:
        logger.warning(
            "Redis Cluster mode failed or inactive (%s). Falling back to standalone connection.",
            exc,
        )
        first_host, first_port = cfg.parsed_redis_nodes[0]
        try:
            pool = ConnectionPool(
                host=first_host,
                port=first_port,
                password=redis_password,
                max_connections=cfg.REDIS_MAX_CONNECTIONS,
                socket_timeout=cfg.REDIS_SOCKET_TIMEOUT,
                socket_connect_timeout=cfg.REDIS_CONNECT_TIMEOUT,
                decode_responses=False,
            )
            standalone = Redis(connection_pool=pool)
            await standalone.ping()
        except AuthenticationError as auth_err:
            if redis_password and "without any password configured" in str(auth_err).lower():
                logger.warning("Standalone Redis has no password configured; retrying connection without password.")
                pool = ConnectionPool(
                    host=first_host,
                    port=first_port,
                    password=None,
                    max_connections=cfg.REDIS_MAX_CONNECTIONS,
                    socket_timeout=cfg.REDIS_SOCKET_TIMEOUT,
                    socket_connect_timeout=cfg.REDIS_CONNECT_TIMEOUT,
                    decode_responses=False,
                )
                standalone = Redis(connection_pool=pool)
                await standalone.ping()
            else:
                raise
        logger.info("Connected to standalone fallback Redis at %s:%d", first_host, first_port)
        return standalone


async def close_redis_client(client: Optional[Union[RedisCluster, Redis]]) -> None:
    """Gracefully shuts down Redis client connections."""
    if client is None:
        return
    try:
        await client.aclose()
        logger.info("Redis client connections closed successfully.")
    except Exception as exc:
        logger.warning("Error during Redis client shutdown: %s", exc)


# ==============================================================================
# Temporary State Service: Idempotency, Challenges, Distributed Locks
# ==============================================================================


class RedisTemporaryState:
    """Manages short-lived temporary state, distributed locks, challenges, and idempotency."""

    def __init__(self, redis: Union[RedisCluster, Redis]) -> None:
        self.redis: Union[RedisCluster, Redis] = redis
        self._release_lock_sha: Optional[str] = None

    async def _get_release_sha(self) -> str:
        if self._release_lock_sha is None:
            try:
                self._release_lock_sha = await self.redis.script_load(RELEASE_LOCK_LUA)
            except Exception:
                self._release_lock_sha = ""
        return self._release_lock_sha

    # ---- Distributed Locks ----

    async def acquire_lock(self, resource: str, ttl_ms: int = 5000) -> Optional[str]:
        """Atomically acquires a distributed lock using SET NX PX with a unique nonce.

        Returns the lock ownership token if acquired, or None if busy.
        """
        token = os.urandom(16).hex()
        lock_key = f"lock:{resource}"
        try:
            acquired = await self.redis.set(lock_key, token, px=ttl_ms, nx=True)
            return token if acquired else None
        except (RedisConnError, RedisTimeoutError) as exc:
            logger.warning("Redis lock acquisition failed on %s: %s", lock_key, exc)
            return None

    async def release_lock(self, resource: str, token: str) -> bool:
        """Atomically releases lock ownership only if the token matches."""
        lock_key = f"lock:{resource}"
        try:
            sha = await self._get_release_sha()
            if sha:
                result = await self.redis.evalsha(sha, 1, lock_key, token)
            else:
                result = await self.redis.eval(RELEASE_LOCK_LUA, 1, lock_key, token)
            return bool(result)
        except Exception as exc:
            logger.warning("Error releasing lock %s: %s", lock_key, exc)
            return False

    # ---- Idempotency Keys ----

    async def acquire_idempotency_key(
        self,
        key: str,
        ttl_seconds: int = 86400,
    ) -> Tuple[bool, Optional[Dict[str, Any]]]:
        """Checks and reserves an idempotency key.

        Returns:
            (True, None) if key was freshly acquired.
            (False, cached_result) if request was already processed or in-flight.
        """
        redis_key = f"idempotency:{key}"
        try:
            # Check if previous response exists
            existing = await self.redis.get(redis_key)
            if existing is not None:
                try:
                    cached = orjson.loads(existing)
                    return False, cached
                except Exception:
                    return False, {"status": "in_progress"}

            # Mark in-progress atomically with pending sentinel
            acquired = await self.redis.set(
                redis_key,
                orjson.dumps({"status": "pending", "acquired_at": time.time()}),
                ex=ttl_seconds,
                nx=True,
            )
            return (bool(acquired), None)
        except Exception as exc:
            logger.warning("Idempotency check failed for key %s: %s", key, exc)
            return True, None

    async def save_idempotency_result(
        self,
        key: str,
        result: Dict[str, Any],
        ttl_seconds: int = 86400,
    ) -> bool:
        """Stores completed execution payload under idempotency key."""
        redis_key = f"idempotency:{key}"
        try:
            payload = orjson.dumps(result)
            await self.redis.set(redis_key, payload, ex=ttl_seconds)
            return True
        except Exception as exc:
            logger.warning("Failed saving idempotency result for %s: %s", key, exc)
            return False

    # ---- Verification Challenges & Tokens ----

    async def create_challenge(
        self,
        scope: str,
        token: str,
        data: Dict[str, Any],
        ttl_seconds: int = 300,
    ) -> bool:
        """Stores short-lived verification token payload."""
        redis_key = f"challenge:{scope}:{token}"
        try:
            payload = orjson.dumps(data)
            await self.redis.set(redis_key, payload, ex=ttl_seconds)
            return True
        except Exception as exc:
            logger.warning("Failed saving verification challenge %s: %s", scope, exc)
            return False

    async def verify_challenge(
        self,
        scope: str,
        token: str,
        consume: bool = True,
    ) -> Optional[Dict[str, Any]]:
        """Verifies and optionally consumes (deletes) verification token."""
        redis_key = f"challenge:{scope}:{token}"
        try:
            val = await self.redis.get(redis_key)
            if val is None:
                return None
            if consume:
                await self.redis.delete(redis_key)
            return orjson.loads(val)
        except Exception as exc:
            logger.warning("Error verifying challenge %s: %s", scope, exc)
            return None

    # ---- Frequently Updated Counters ----

    async def incr_counter(
        self,
        key: str,
        amount: int = 1,
        ttl_seconds: Optional[int] = None,
    ) -> int:
        """Atomically increments a counter and optionally sets expiration."""
        redis_key = f"counter:{key}"
        try:
            new_val = await self.redis.incrby(redis_key, amount)
            if ttl_seconds is not None:
                await self.redis.expire(redis_key, ttl_seconds)
            return int(new_val)
        except Exception as exc:
            logger.warning("Failed incrementing counter %s: %s", key, exc)
            return 0


# ==============================================================================
# Reusable @cached Decorator for Service Layer
# ==============================================================================


def cached(
    ttl: int = 300,
    prefix: str = "cache",
    enabled: bool = True,
    jitter: bool = True,
    key_builder: Optional[Callable[..., str]] = None,
) -> Callable[..., Any]:
    """Decorator providing transparent Cache-Aside caching with JSON serialization.

    Per-call bypass: pass `cache_enabled=False` to bypass reading and writing the cache.
    """

    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        @functools.wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            call_enabled = kwargs.pop("cache_enabled", None)
            is_active = enabled if call_enabled is None else call_enabled

            # Resolve Redis client from self/cls instance or keyword arguments
            instance = args[0] if args else None
            redis_client: Optional[Union[RedisCluster, Redis]] = None

            if instance and hasattr(instance, "cache") and hasattr(instance.cache, "redis"):
                redis_client = instance.cache.redis
            elif instance and hasattr(instance, "redis"):
                redis_client = instance.redis

            if not is_active or redis_client is None:
                return await func(*args, **kwargs)

            # Generate cache key
            if key_builder is not None:
                cache_key = key_builder(*args, **kwargs)
            else:
                arg_repr = [str(a) for a in args[1:]]  # skip self
                kwarg_repr = [f"{k}={v}" for k, v in sorted(kwargs.items())]
                key_raw = f"{prefix}:{func.__name__}:{':'.join(arg_repr)}:{':'.join(kwarg_repr)}"
                hashed = hashlib.sha256(key_raw.encode("utf-8")).hexdigest()[:16]
                cache_key = f"{prefix}:{func.__name__}:{hashed}"

            # 1. Try cache read
            try:
                raw = await redis_client.get(cache_key)
                if raw is not None:
                    return orjson.loads(raw)
            except Exception as exc:
                logger.warning("Cache read failed for key %s: %s", cache_key, exc)

            # 2. Compute result
            result = await func(*args, **kwargs)

            # 3. Store computed result
            if result is not None:
                try:
                    final_ttl = ttl
                    if jitter:
                        spread = ttl * 0.10
                        final_ttl = max(1, int(ttl + random.uniform(-spread, spread)))
                    payload = orjson.dumps(result)
                    await redis_client.set(cache_key, payload, ex=final_ttl)
                except Exception as exc:
                    logger.warning("Cache write failed for key %s: %s", cache_key, exc)

            return result

        return wrapper

    return decorator


__all__ = [
    "create_redis_client",
    "close_redis_client",
    "RedisTemporaryState",
    "cached",
    "RELEASE_LOCK_LUA",
]
