"""Abstract interface for Generic Asynchronous Repositories following OOP SOLID principles.

Defines the contract (Interface Segregation & Dependency Inversion) for all repository implementations.
Concrete repositories implement this interface to ensure polymorphic behavior and testability.
"""

from abc import ABC, abstractmethod
from datetime import datetime
from typing import (
    Any,
    Dict,
    Generic,
    List,
    Optional,
    Sequence,
    Tuple,
    Type,
    TypeVar,
    Union,
)

from pydantic import BaseModel

from app.db.base import Base

ModelType = TypeVar("ModelType", bound=Base)
IdType = TypeVar("IdType")


class IBaseRepository(ABC, Generic[ModelType, IdType]):
    """Abstract Base Repository interface defining full dynamic data access operations."""

    @property
    @abstractmethod
    def model(self) -> Type[ModelType]:
        """Returns the SQLAlchemy model class managed by this repository."""
        pass

    @abstractmethod
    async def get_by_id(
        self,
        obj_id: Any,
        options: Optional[Sequence[Any]] = None,
    ) -> Optional[ModelType]:
        """Retrieves an entity by its primary key with optional eager/lazy load options."""
        pass

    @abstractmethod
    async def get(
        self,
        entity_id: IdType,
        options: Optional[Sequence[Any]] = None,
    ) -> Optional[ModelType]:
        """Retrieves an entity by its primary key (alias for get_by_id)."""
        pass

    @abstractmethod
    async def get_all(
        self,
        limit: int = 100,
        offset: int = 0,
        options: Optional[Sequence[Any]] = None,
        **filters: Any,
    ) -> List[ModelType]:
        """Retrieves all entities matching filters with limit and offset."""
        pass

    @abstractmethod
    async def get_by(
        self,
        options: Optional[Sequence[Any]] = None,
        **kwargs: Any,
    ) -> Optional[ModelType]:
        """Dynamically finds the first entity matching arbitrary column expressions."""
        pass

    @abstractmethod
    async def exists_by(self, **kwargs: Any) -> bool:
        """Checks if any entity exists matching dynamic filter criteria."""
        pass

    @abstractmethod
    async def count(self, **filters: Any) -> int:
        """Counts records matching dynamic filter criteria."""
        pass

    @abstractmethod
    async def create(
        self,
        data: Union[BaseModel, Dict[str, Any], ModelType],
        refresh: bool = False,
    ) -> ModelType:
        """Adds and flushes a new entity to the active transaction."""
        pass

    @abstractmethod
    async def bulk_create(
        self,
        items: Sequence[Union[BaseModel, Dict[str, Any], ModelType]],
    ) -> Sequence[ModelType]:
        """Adds and flushes multiple entities to the active transaction in a single round-trip."""
        pass

    @abstractmethod
    async def update(
        self,
        entity: ModelType,
        data: Optional[Union[BaseModel, Dict[str, Any]]] = None,
        **kwargs: Any,
    ) -> ModelType:
        """Dynamically updates attributes on an existing entity and flushes."""
        pass

    @abstractmethod
    async def delete(self, entity: ModelType) -> None:
        """Removes an entity from the active transaction and flushes."""
        pass

    @abstractmethod
    async def delete_by_id(self, entity_id: IdType) -> bool:
        """Looks up an entity by primary key and deletes it if found."""
        pass

    @abstractmethod
    async def delete_where(self, **filters: Any) -> int:
        """Deletes all entities matching dynamic criteria and returns count of deleted rows."""
        pass

    @abstractmethod
    async def list(
        self,
        offset: int = 0,
        limit: int = 100,
        order_by: Union[str, Sequence[str], None] = None,
        descending: bool = False,
        options: Optional[Sequence[Any]] = None,
        **filters: Any,
    ) -> Sequence[ModelType]:
        """Dynamic offset-based pagination with rich operator filtering and multi-column ordering."""
        pass

    @abstractmethod
    async def list_keyset(
        self,
        limit: int = 50,
        cursor_dt: Optional[datetime] = None,
        cursor_id: Optional[Any] = None,
        order_col: str = "created_at",
        id_col: str = "id",
        options: Optional[Sequence[Any]] = None,
        **filters: Any,
    ) -> Tuple[List[ModelType], bool]:
        """Dynamic keyset (cursor) pagination across arbitrary model ordering columns."""
        pass

    @abstractmethod
    async def aggregate(
        self,
        func_name: str,
        column_name: str,
        **filters: Any,
    ) -> Any:
        """Executes an aggregate function (sum, avg, min, max, count) dynamically."""
        pass

    @abstractmethod
    async def flush(self) -> None:
        """Flushes pending state changes to the database without committing."""
        pass
