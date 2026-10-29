"""Unit tests for DynamicRedisCache: dynamic flags, namespace versioning, negative caching, and stampede locks."""

from typing import Any, Dict, Optional
import pytest

from app.core.config import CacheOperationFlags
from app.services.base import (
    NEGATIVE_CACHE_SENTINEL,
    DynamicRedisCache,
)


@pytest.mark.asyncio
async def test_item_crud_operations(mock_redis: Any) -> None:
    """Tests basic get, set, and delete operations on single item cache."""
    cache: DynamicRedisCache = DynamicRedisCache(redis=mock_redis, prefix="test")

    # Miss
    assert await cache.get_item("123") is None

    # Set
    item_data: Dict[str, Any] = {"id": "123", "name": "Alice"}
    await cache.set_item("123", item_data)

    # Hit
    retrieved: Optional[Dict[str, Any]] = await cache.get_item("123")
    assert retrieved == item_data

    # Delete
    await cache.delete_item("123")
    assert await cache.get_item("123") is None


@pytest.mark.asyncio
async def test_dynamic_per_operation_flags(mock_redis: Any) -> None:
    """Verifies that granular feature flags disable individual cache operations dynamically."""
    flags: CacheOperationFlags = CacheOperationFlags(
        enable_get_item=False,  # Get disabled
        enable_set_item=True,
        enable_delete_item=True,
    )
    cache: DynamicRedisCache = DynamicRedisCache(redis=mock_redis, prefix="test", flags=flags)

    await cache.set_item("user_1", {"name": "Bob"})

    # Even though data exists in Redis, get_item returns None because flag is False
    assert await cache.get_item("user_1") is None

    # Flip flag back on dynamically
    cache.flags.enable_get_item = True
    assert (await cache.get_item("user_1")) == {"name": "Bob"}

    # Test set_item flag disabled
    cache.flags.enable_set_item = False
    await cache.set_item("user_2", {"name": "Charlie"})
    cache.flags.enable_get_item = True
    assert await cache.get_item("user_2") is None


@pytest.mark.asyncio
async def test_negative_caching(mock_redis: Any) -> None:
    """Verifies non-existent items are cached with sentinel to protect database."""
    cache: DynamicRedisCache = DynamicRedisCache(redis=mock_redis, prefix="test", negative_ttl=30)

    # Cache negative result
    await cache.set_negative_cache("missing_id")

    # Subsequent lookup returns negative cache sentinel
    cached: Optional[Dict[str, Any]] = await cache.get_item("missing_id")
    assert cached == NEGATIVE_CACHE_SENTINEL


def test_filter_normalization_and_hashing(mock_redis: Any) -> None:
    """Ensures filter order independence and non-whitelisted param stripping."""
    cache: DynamicRedisCache = DynamicRedisCache(
        redis=mock_redis, prefix="test", allowed_filter_keys={"role", "limit"}
    )

    # Reordered params must produce identical SHA-256 hash
    hash1: str = cache._hash_filters(role="admin", limit=50)
    hash2: str = cache._hash_filters(limit=50, role="admin")
    assert hash1 == hash2

    # Non-whitelisted params (e.g. tracking_token) are discarded
    hash3: str = cache._hash_filters(role="admin", limit=50, tracking_token="xyz123")
    assert hash1 == hash3


@pytest.mark.asyncio
async def test_namespace_versioning_list_invalidation(mock_redis: Any) -> None:
    """Verifies O(1) list invalidation where incrementing list_ver invalidates all page keys instantly."""
    cache: DynamicRedisCache = DynamicRedisCache(redis=mock_redis, prefix="users")

    page_data: Dict[str, Any] = {"items": [{"id": "1"}], "next_cursor": None}

    # Store list under version 1
    await cache.set_paginated(page_data, role="admin", limit=50)

    # Cache hit
    hit: Optional[Dict[str, Any]] = await cache.get_paginated(role="admin", limit=50)
    assert hit == page_data

    # Invalidate lists (atomic INCR on list_ver)
    await cache.invalidate_lists()

    # Immediate cache miss for the exact same query parameters without any DEL on the list key!
    miss: Optional[Dict[str, Any]] = await cache.get_paginated(role="admin", limit=50)
    assert miss is None


@pytest.mark.asyncio
async def test_stampede_protection_lock(mock_redis: Any) -> None:
    """Verifies get_or_compute_item acquires lock, computes value, stores in cache, and releases lock."""
    cache: DynamicRedisCache = DynamicRedisCache(redis=mock_redis, prefix="test")
    compute_called: int = 0

    async def _compute() -> Dict[str, Any]:
        nonlocal compute_called
        compute_called += 1
        return {"id": "hot_key", "value": "computed_result"}

    # First access computes and caches
    result1: Optional[Dict[str, Any]] = await cache.get_or_compute_item("hot_key", _compute)
    assert result1 == {"id": "hot_key", "value": "computed_result"}
    assert compute_called == 1

    # Second access reads directly from cache, does not call compute
    result2: Optional[Dict[str, Any]] = await cache.get_or_compute_item("hot_key", _compute)
    assert result2 == {"id": "hot_key", "value": "computed_result"}
    assert compute_called == 1  # Still 1!
