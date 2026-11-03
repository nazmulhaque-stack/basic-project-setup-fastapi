"""Abstract cache interface and Null Object implementation.

Lives in `app/core/` — not `app/services/` — specifically so both
`app/repositories/base.py` (the dynamic repository's per-call query cache)
and `app/services/*.py` (item/list cache-aside) can depend on it without
either layer importing the other's package. `app/services/interface.py`
re-exports these two names for backward compatibility with existing imports.

Includes the Null Object Pattern (`NullCache`) to eliminate `if self.cache is
not None:` checks, enabling true polymorphism across cached and non-cached
code paths.
"""

from abc import ABC, abstractmethod
from typing import Any, Callable, Dict, Optional


class ICacheService(ABC):
    """Abstract interface for distributed and in-memory cache operations."""

    @abstractmethod
    async def get_item(
        self,
        entity_id: Any,
        enabled: Optional[bool] = None,
    ) -> Optional[Dict[str, Any]]:
        """Retrieves an item from cache by key."""
        pass

    @abstractmethod
    async def set_item(
        self,
        entity_id: Any,
        data: Dict[str, Any],
        enabled: Optional[bool] = None,
    ) -> None:
        """Stores an item in cache."""
        pass

    @abstractmethod
    async def delete_item(
        self,
        entity_id: Any,
        enabled: Optional[bool] = None,
    ) -> None:
        """Deletes an item from cache."""
        pass

    @abstractmethod
    async def get_or_compute_item(
        self,
        entity_id: Any,
        compute_func: Callable[[], Any],
        enabled: Optional[bool] = None,
    ) -> Optional[Dict[str, Any]]:
        """Atomically retrieves an item or executes compute_func protected by stampede locks."""
        pass

    @abstractmethod
    async def get_paginated(
        self,
        enabled: Optional[bool] = None,
        **filter_params: Any,
    ) -> Optional[Dict[str, Any]]:
        """Retrieves a cached paginated list page."""
        pass

    @abstractmethod
    async def set_paginated(
        self,
        data: Dict[str, Any],
        enabled: Optional[bool] = None,
        **filter_params: Any,
    ) -> None:
        """Caches a paginated list page under the current namespace version."""
        pass

    @abstractmethod
    async def invalidate_lists(
        self,
        enabled: Optional[bool] = None,
    ) -> None:
        """Atomically invalidates all cached list pages in O(1) time."""
        pass

    # Core key-value cache primitives
    @abstractmethod
    async def get(self, key: str) -> Optional[Any]:
        """Gets a deserialized JSON value by raw key."""
        pass

    @abstractmethod
    async def set(self, key: str, value: Any, ttl: Optional[int] = None) -> bool:
        """Sets a JSON-serializable value under a raw key with optional TTL."""
        pass

    @abstractmethod
    async def delete(self, key: str) -> bool:
        """Deletes a key from cache."""
        pass

    @abstractmethod
    async def exists(self, key: str) -> bool:
        """Checks if a key exists in cache."""
        pass

    @abstractmethod
    async def expire(self, key: str, ttl: int) -> bool:
        """Sets expiration TTL on a key."""
        pass

    @abstractmethod
    async def ttl(self, key: str) -> int:
        """Returns remaining TTL of a key in seconds (-2 if not found, -1 if no TTL)."""
        pass

    @abstractmethod
    async def incr(self, key: str) -> int:
        """Atomically increments an integer counter key, returning the new value
        (creating it at 1 if absent). Used for O(1) namespace-version cache
        invalidation — e.g. `BaseRepository`'s per-table dynamic query cache."""
        pass


class NullCache(ICacheService):
    """Null Object implementation of ICacheService.

    Provides safe no-op behavior when caching is absent or disabled,
    eliminating fragile `if self.cache is not None:` conditionals throughout service code.
    """

    async def get_item(self, entity_id: Any, enabled: Optional[bool] = None) -> Optional[Dict[str, Any]]:
        return None

    async def set_item(self, entity_id: Any, data: Dict[str, Any], enabled: Optional[bool] = None) -> None:
        pass

    async def delete_item(self, entity_id: Any, enabled: Optional[bool] = None) -> None:
        pass

    async def get_or_compute_item(
        self,
        entity_id: Any,
        compute_func: Callable[[], Any],
        enabled: Optional[bool] = None,
    ) -> Optional[Dict[str, Any]]:
        return await compute_func()

    async def get_paginated(self, enabled: Optional[bool] = None, **filter_params: Any) -> Optional[Dict[str, Any]]:
        return None

    async def set_paginated(self, data: Dict[str, Any], enabled: Optional[bool] = None, **filter_params: Any) -> None:
        pass

    async def invalidate_lists(self, enabled: Optional[bool] = None) -> None:
        pass

    async def get(self, key: str) -> Optional[Any]:
        return None

    async def set(self, key: str, value: Any, ttl: Optional[int] = None) -> bool:
        return True

    async def delete(self, key: str) -> bool:
        return True

    async def exists(self, key: str) -> bool:
        return False

    async def expire(self, key: str, ttl: int) -> bool:
        return True

    async def ttl(self, key: str) -> int:
        return -2

    async def incr(self, key: str) -> int:
        return 1


__all__ = ["ICacheService", "NullCache"]
