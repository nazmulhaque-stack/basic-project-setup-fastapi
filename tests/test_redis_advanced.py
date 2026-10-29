"""Comprehensive unit and integration test suite for Redis caching, temporary state, and locks."""

import asyncio
from typing import Any, Dict, Optional
from unittest.mock import AsyncMock, MagicMock
import pytest

from app.core.redis import RedisTemporaryState, cached
from app.services.base import DynamicRedisCache
from app.services.interface import NullCache


class InMemoryMockRedis:
    """Accurate in-memory mock of Redis client implementing string and script primitives."""

    def __init__(self) -> None:
        self.store: Dict[str, bytes] = {}
        self.ttls: Dict[str, int] = {}
        self.scripts: Dict[str, str] = {}

    async def get(self, key: str) -> Optional[bytes]:
        return self.store.get(key)

    async def set(
        self,
        key: str,
        value: bytes,
        ex: Optional[int] = None,
        px: Optional[int] = None,
        nx: bool = False,
    ) -> bool:
        if nx and key in self.store:
            return False
        self.store[key] = value if isinstance(value, bytes) else str(value).encode("utf-8")
        if ex is not None:
            self.ttls[key] = ex
        elif px is not None:
            self.ttls[key] = max(1, px // 1000)
        return True

    async def delete(self, *keys: str) -> int:
        deleted = 0
        for k in keys:
            if k in self.store:
                del self.store[k]
                self.ttls.pop(k, None)
                deleted += 1
        return deleted

    async def exists(self, *keys: str) -> int:
        return sum(1 for k in keys if k in self.store)

    async def expire(self, key: str, seconds: int) -> bool:
        if key in self.store:
            self.ttls[key] = seconds
            return True
        return False

    async def ttl(self, key: str) -> int:
        if key not in self.store:
            return -2
        return self.ttls.get(key, -1)

    async def incrby(self, key: str, amount: int = 1) -> int:
        val = int(self.store.get(key, b"0").decode("utf-8")) + amount
        self.store[key] = str(val).encode("utf-8")
        return val

    async def incr(self, key: str) -> int:
        return await self.incrby(key, 1)

    async def script_load(self, script: str) -> str:
        sha = f"sha_{len(self.scripts)}"
        self.scripts[sha] = script
        return sha

    async def evalsha(self, sha: str, numkeys: int, *args: Any) -> Any:
        # Emulate lock release script
        key = args[0]
        token = args[1]
        current = self.store.get(key, b"").decode("utf-8")
        if current == str(token):
            del self.store[key]
            return 1
        return 0

    async def eval(self, script: str, numkeys: int, *args: Any) -> Any:
        return await self.evalsha("direct", numkeys, *args)

    async def ping(self) -> bool:
        return True

    async def aclose(self) -> None:
        pass


@pytest.fixture
def mock_redis() -> InMemoryMockRedis:
    return InMemoryMockRedis()


@pytest.mark.asyncio
async def test_cache_core_primitives(mock_redis: InMemoryMockRedis) -> None:
    """Verifies get, set, delete, exists, expire, and ttl operations on DynamicRedisCache."""
    cache = DynamicRedisCache(redis=mock_redis, prefix="test")

    # Set
    success = await cache.set("my_key", {"status": "ok", "count": 42}, ttl=60)
    assert success is True

    # Exists & TTL
    assert await cache.exists("my_key") is True
    assert await cache.ttl("my_key") == 60

    # Get
    val = await cache.get("my_key")
    assert val == {"status": "ok", "count": 42}

    # Expire
    await cache.expire("my_key", 120)
    assert await cache.ttl("my_key") == 120

    # Delete
    del_ok = await cache.delete("my_key")
    assert del_ok is True
    assert await cache.get("my_key") is None
    assert await cache.exists("my_key") is False


@pytest.mark.asyncio
async def test_null_cache_primitives() -> None:
    """Verifies NullCache safe polymorphism for core primitives."""
    cache = NullCache()
    assert await cache.get("k") is None
    assert await cache.set("k", "v") is True
    assert await cache.delete("k") is True
    assert await cache.exists("k") is False
    assert await cache.expire("k", 60) is True
    assert await cache.ttl("k") == -2


@pytest.mark.asyncio
async def test_cached_decorator(mock_redis: InMemoryMockRedis) -> None:
    """Verifies @cached decorator caching, per-call bypass, and invocation counters."""

    class MockCatalogService:
        def __init__(self, redis: InMemoryMockRedis) -> None:
            self.redis = redis
            self.db_calls = 0

        @cached(ttl=300, prefix="products")
        async def fetch_details(self, product_id: int) -> Dict[str, Any]:
            self.db_calls += 1
            return {"id": product_id, "title": f"Product {product_id}"}

    service = MockCatalogService(mock_redis)

    # First call: cache miss, executes function
    res1 = await service.fetch_details(101)
    assert res1["id"] == 101
    assert service.db_calls == 1

    # Second call: cache hit, returns from Redis without calling function
    res2 = await service.fetch_details(101)
    assert res2 == res1
    assert service.db_calls == 1

    # Third call with cache_enabled=False: bypasses cache
    res3 = await service.fetch_details(101, cache_enabled=False)
    assert res3 == res1
    assert service.db_calls == 2


@pytest.mark.asyncio
async def test_idempotency_keys(mock_redis: InMemoryMockRedis) -> None:
    """Verifies idempotency reservation and payload caching."""
    temp_state = RedisTemporaryState(mock_redis)
    key = "order_req_abc_123"

    # First attempt: successfully acquired
    acquired, cached_data = await temp_state.acquire_idempotency_key(key, ttl_seconds=3600)
    assert acquired is True
    assert cached_data is None

    # Save final completed payload
    saved = await temp_state.save_idempotency_result(key, {"order_id": "ord_999", "status": "completed"})
    assert saved is True

    # Duplicate attempt: returns already completed payload
    acquired_dup, cached_dup = await temp_state.acquire_idempotency_key(key, ttl_seconds=3600)
    assert acquired_dup is False
    assert cached_dup is not None
    assert cached_dup["order_id"] == "ord_999"


@pytest.mark.asyncio
async def test_verification_challenges(mock_redis: InMemoryMockRedis) -> None:
    """Verifies short-lived challenge tokens creation and atomic consumption."""
    temp_state = RedisTemporaryState(mock_redis)

    created = await temp_state.create_challenge(
        scope="email_verify",
        token="tok_secret_777",
        data={"user_id": 42, "email": "test@example.com"},
        ttl_seconds=300,
    )
    assert created is True

    # Verify and consume
    verified = await temp_state.verify_challenge("email_verify", "tok_secret_777", consume=True)
    assert verified is not None
    assert verified["user_id"] == 42

    # Second check fails because token was consumed
    consumed_check = await temp_state.verify_challenge("email_verify", "tok_secret_777")
    assert consumed_check is None


@pytest.mark.asyncio
async def test_distributed_locks_mutual_exclusion(mock_redis: InMemoryMockRedis) -> None:
    """Verifies atomic distributed lock acquisition and owner-guarded release."""
    temp_state = RedisTemporaryState(mock_redis)
    resource = "inventory:item:404"

    # Worker 1 acquires lock
    token1 = await temp_state.acquire_lock(resource, ttl_ms=5000)
    assert token1 is not None

    # Worker 2 attempts to acquire same resource -> blocked (returns None)
    token2 = await temp_state.acquire_lock(resource, ttl_ms=5000)
    assert token2 is None

    # Worker 2 attempts to release using incorrect token -> fails
    release_invalid = await temp_state.release_lock(resource, "wrong_token")
    assert release_invalid is False

    # Worker 1 releases using valid token -> succeeds
    release_valid = await temp_state.release_lock(resource, token1)
    assert release_valid is True

    # Now Worker 2 can acquire lock
    token2_retry = await temp_state.acquire_lock(resource, ttl_ms=5000)
    assert token2_retry is not None
    await temp_state.release_lock(resource, token2_retry)


@pytest.mark.asyncio
async def test_atomic_counters(mock_redis: InMemoryMockRedis) -> None:
    """Verifies high-frequency atomic counter increments."""
    temp_state = RedisTemporaryState(mock_redis)

    c1 = await temp_state.incr_counter("page_views:home", amount=1, ttl_seconds=60)
    assert c1 == 1

    c2 = await temp_state.incr_counter("page_views:home", amount=5)
    assert c2 == 6
