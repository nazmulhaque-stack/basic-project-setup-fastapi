"""Abstract Service interface following Object-Oriented SOLID principles.

`ICacheService`/`NullCache` used to be defined here, but now live in
`app/core/cache_interface.py` so `app/repositories/base.py` can depend on
them too without creating a cycle through this package's `__init__.py`
(which eagerly imports the concrete `*Service` classes, which import the
concrete `*Repository` classes, which — before this split — would import
back into this very module while it was still mid-import). They're
re-exported below for every existing `from app.services.interface import
ICacheService, NullCache` call site.
"""

from abc import ABC, abstractmethod
from typing import (
    Any,
    Dict,
    Generic,
    List,
    Optional,
    TypeVar,
)

from pydantic import BaseModel
from app.core.cache_interface import ICacheService, NullCache
from app.models.base import Base

ModelType = TypeVar("ModelType", bound=Base)
CreateSchemaType = TypeVar("CreateSchemaType", bound=BaseModel)
UpdateSchemaType = TypeVar("UpdateSchemaType", bound=BaseModel)
ReadSchemaType = TypeVar("ReadSchemaType", bound=BaseModel)
IdType = TypeVar("IdType")


class IBaseService(ABC, Generic[ModelType, CreateSchemaType, UpdateSchemaType, ReadSchemaType, IdType]):
    """Abstract interface defining the contract for Domain Service Orchestrators."""

    @abstractmethod
    async def create(
        self,
        payload: CreateSchemaType,
        cache_enabled: Optional[bool] = None,
        emit_outbox_event: Optional[bool] = None,
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
        emit_outbox_event: Optional[bool] = None,
    ) -> Optional[ReadSchemaType]:
        """Updates an entity by ID, returning updated read schema or None if not found."""
        pass

    @abstractmethod
    async def delete(
        self,
        entity_id: IdType,
        cache_enabled: Optional[bool] = None,
        emit_outbox_event: Optional[bool] = None,
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


__all__ = ["ICacheService", "NullCache", "IBaseService"]
