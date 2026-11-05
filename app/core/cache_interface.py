"""Abstract cache interface and Null Object implementation."""

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
    ) -> bool:
        """Stores an item in cache."""
        pass

    @abstractmethod
    async def delete_item(
        self,
        entity_id: Any,
        enabled: Optional[bool] = None,
    ) -> bool:
        """Deletes an item from cache."""
        pass

    @abstractmethod
    async def set_negative_cache(
        self,
        entity_id: Any,
        enabled: Optional[bool] = None,
    ) -> bool:
        """Caches confirmed non-existence to prevent DB penetration."""
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
    ) -> bool:
        """Caches a paginated list page under the current namespace version."""
        pass

    @abstractmethod
    async def invalidate_lists(
        self,
        enabled: Optional[bool] = None,
    ) -> bool:
        """Invalidates all cached list pages in O(1) time."""
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
    async def get(self, key: str) -> Optional[Any]:
        """Retrieves and deserializes value by raw key."""
        pass

    @abstractmethod
    async def set(self, key: str, value: Any, ttl: Optional[int] = None) -> bool:
        """Stores value under raw key."""
        pass

    @abstractmethod
    async def delete(self, key: str) -> bool:
        """Deletes raw key from cache."""
        pass

    @abstractmethod
    async def exists(self, key: str) -> bool:
        """Checks if key exists in cache."""
        pass

    @abstractmethod
    async def expire(self, key: str, ttl: int) -> bool:
        """Sets TTL on key."""
        pass

    @abstractmethod
    async def ttl(self, key: str) -> int:
        """Returns remaining TTL on key in seconds (-2 if not found)."""
        pass

    @abstractmethod
    async def incr(self, key: str) -> int:
        """Increments a counter key."""
        pass


class NullCache(ICacheService):
    """Null Object implementation of ICacheService.

    Provides safe no-op behavior when caching is absent or disabled.
    """

    async def get_item(self, entity_id: Any, enabled: Optional[bool] = None) -> Optional[Dict[str, Any]]:
        return None

    async def set_item(self, entity_id: Any, data: Dict[str, Any], enabled: Optional[bool] = None) -> bool:
        return True

    async def delete_item(self, entity_id: Any, enabled: Optional[bool] = None) -> bool:
        return True

    async def set_negative_cache(self, entity_id: Any, enabled: Optional[bool] = None) -> bool:
        return True

    async def get_paginated(self, enabled: Optional[bool] = None, **filter_params: Any) -> Optional[Dict[str, Any]]:
        return None

    async def set_paginated(self, data: Dict[str, Any], enabled: Optional[bool] = None, **filter_params: Any) -> bool:
        return True

    async def invalidate_lists(self, enabled: Optional[bool] = None) -> bool:
        return True

    async def get_or_compute_item(
        self,
        entity_id: Any,
        compute_func: Callable[[], Any],
        enabled: Optional[bool] = None,
    ) -> Optional[Dict[str, Any]]:
        return await compute_func()

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
