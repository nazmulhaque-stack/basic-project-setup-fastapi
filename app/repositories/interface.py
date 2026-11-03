"""Abstract interface for hand-written asyncpg repositories.

Each concrete repository (UserRepository, ProductRepository, ...) implements
its own SQL for every method below — there is no generic, dynamically
dispatched filter engine. The interface exists only so `BaseService` depends
on a stable shape rather than any one repository's concrete class.
"""

from abc import ABC, abstractmethod
from collections.abc import Mapping, Sequence
from datetime import datetime
from typing import Any, Generic, List, Optional, Tuple, Type, TypeVar

from app.models.base import Base

ModelType = TypeVar("ModelType", bound=Base)
IdType = TypeVar("IdType")


class IBaseRepository(ABC, Generic[ModelType, IdType]):
    """Abstract repository contract implemented by hand-written asyncpg repositories."""

    @property
    @abstractmethod
    def model(self) -> Type[ModelType]:
        """Returns the dataclass this repository constructs rows into."""
        pass

    @abstractmethod
    async def get(self, entity_id: IdType) -> Optional[ModelType]:
        """Retrieves an entity by its primary key."""
        pass

    @abstractmethod
    async def create(self, entity: ModelType) -> ModelType:
        """Inserts a new row and returns the fully-populated entity (via RETURNING *)."""
        pass

    @abstractmethod
    async def update(self, entity: ModelType) -> ModelType:
        """Updates an existing row from the entity's current field values."""
        pass

    @abstractmethod
    async def delete(self, entity: ModelType) -> None:
        """Deletes a row."""
        pass

    @abstractmethod
    async def list(
        self,
        *,
        filters: Optional[Mapping[str, Any]] = None,
        sort: Optional[Sequence[str]] = None,
        offset: int = 0,
        limit: int = 100,
    ) -> Sequence[ModelType]:
        """Offset-paginated listing using this entity's own supported filter/sort keys."""
        pass

    @abstractmethod
    async def count(self, filters: Optional[Mapping[str, Any]] = None) -> int:
        """Counts rows matching this entity's own supported filters."""
        pass

    @abstractmethod
    async def list_keyset(
        self,
        limit: int = 50,
        cursor_dt: Optional[datetime] = None,
        cursor_id: Optional[Any] = None,
        order_col: str = "created_at",
        id_col: str = "id",
        **filters: Any,
    ) -> Tuple[List[ModelType], bool]:
        """Keyset (cursor) pagination: composite (order_col, id_col) seek."""
        pass


__all__ = ["IBaseRepository"]
