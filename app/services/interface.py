"""Abstract Service and Cache interfaces following Object-Oriented SOLID principles.

Includes the Null Object Pattern (NullCache) to eliminate None-checks in services,
enabling true polymorphism across caching infrastructure.
"""

from abc import ABC, abstractmethod
from typing import (
    Any,
    Callable,
    Dict,
    Generic,
    List,
    Optional,
    TypeVar,
)

from pydantic import BaseModel
from app.models.base import Base

ModelType = TypeVar("ModelType", bound=Base)
CreateSchemaType = TypeVar("CreateSchemaType", bound=BaseModel)
UpdateSchemaType = TypeVar("UpdateSchemaType", bound=BaseModel)
ReadSchemaType = TypeVar("ReadSchemaType", bound=BaseModel)
IdType = TypeVar("IdType")


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


class IBaseService(ABC, Generic[ModelType, CreateSchemaType, UpdateSchemaType, ReadSchemaType, IdType]):
    """Abstract interface defining the contract for Domain Service Orchestrators."""

    @abstractmethod
    async def create(
        self,
        payload: CreateSchemaType,
        cache_enabled: Optional[bool] = None,
    ) -> ReadSchemaType:
        """Creates a domain entity and returns its read schema."""
        pass

    @abstractmethod
    async def get(
        self,
        entity_id: IdType,
        cache_enabled: Optional[bool] = None,
    ) -> Optional[ReadSchemaType]:
        """Retrieves an entity by ID returning None if not found."""
        pass

    @abstractmethod
    async def get_by_id(
        self,
        entity_id: IdType,
        cache_enabled: Optional[bool] = None,
    ) -> ReadSchemaType:
        """Retrieves an entity by ID, raising NotFoundError if not found."""
        pass

    @abstractmethod
    async def get_all(
        self,
        limit: int = 100,
        offset: int = 0,
        **filters: Any,
    ) -> List[ReadSchemaType]:
        """Retrieves list of entities with limit, offset, and filters."""
        pass

    @abstractmethod
    async def update(
        self,
        entity_id: IdType,
        payload: UpdateSchemaType,
        cache_enabled: Optional[bool] = None,
    ) -> Optional[ReadSchemaType]:
        """Updates an entity by ID, returning updated read schema or None if not found."""
        pass

    @abstractmethod
    async def delete(
        self,
        entity_id: IdType,
        cache_enabled: Optional[bool] = None,
    ) -> bool:
        """Deletes an entity by ID, returning True if deleted or False if not found."""
        pass

    @abstractmethod
    async def list_keyset(
        self,
        limit: int = 50,
        cursor: Optional[str] = None,
        cache_enabled: Optional[bool] = None,
        **filters: Any,
    ) -> Dict[str, Any]:
        """Lists entities via keyset (cursor) pagination."""
        pass

    @abstractmethod
    async def list_offset(
        self,
        offset: int = 0,
        limit: int = 50,
        order_by: Optional[str] = None,
        descending: bool = False,
        **filters: Any,
    ) -> List[ReadSchemaType]:
        """Lists entities via offset pagination."""
        pass

    @abstractmethod
    async def list(
        self,
        *,
        filters: Optional[Any] = None,
        sort: Optional[Any] = None,
        page: int = 1,
        size: int = 20,
    ) -> Any:
        """Paginated listing returning (items, total) to power CRUDRouter."""
        pass

    @abstractmethod
    async def search(
        self,
        query: Any,
        limit: int = 20,
        offset: int = 0,
    ) -> Any:
        """Offloads search queries to Elasticsearch (CQRS)."""
        pass
