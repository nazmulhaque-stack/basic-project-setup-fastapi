"""Generic service layer.

The service is the "use case" layer and the transaction boundary:
  * converts Pydantic schemas -> plain dicts for the repository
  * raises domain errors (NotFoundError, BusinessRuleError) instead of HTTP errors
  * calls commit() exactly once per write use case
  * exposes hook methods (Template Method pattern) so subclasses add business rules
    without rewriting create/update/delete.

    class ProductService(BaseService[Product, ProductCreate, ProductUpdate]):
        repository_class = ProductRepository

        async def before_create(self, data: dict) -> dict:
            ...business rule...
            return data
"""

from collections.abc import Mapping, Sequence
from typing import Any, ClassVar, Generic, TypeVar

from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql.base import ExecutableOption

from app.core.exceptions import NotFoundError
from app.repositories.base import ModelT, SQLAlchemyRepository

CreateSchemaT = TypeVar("CreateSchemaT", bound=BaseModel)
UpdateSchemaT = TypeVar("UpdateSchemaT", bound=BaseModel)


class BaseService(Generic[ModelT, CreateSchemaT, UpdateSchemaT]):
    repository_class: ClassVar[type[SQLAlchemyRepository[Any]]]

    def __init__(self, session: AsyncSession) -> None:
        self.session = session
        # Every repository a service builds shares this session -> one transaction.
        self.repository: SQLAlchemyRepository[ModelT] = self.repository_class(session)

    @property
    def resource_name(self) -> str:
        return self.repository.model.__name__

    # ------------------------------------------------------------- hooks
    async def before_create(self, data: dict[str, Any]) -> dict[str, Any]:
        return data

    async def before_update(self, obj: ModelT, data: dict[str, Any]) -> dict[str, Any]:
        return data

    async def before_delete(self, obj: ModelT) -> None:
        return None

    # -------------------------------------------------------------- read
    async def get(self, id_: Any, *, options: Sequence[ExecutableOption] = ()) -> ModelT:
        obj = await self.repository.get(id_, options=options)
        if obj is None:
            raise NotFoundError(f"{self.resource_name} with id={id_} not found.")
        return obj

    async def list(
        self,
        *,
        filters: Mapping[str, Any] | None = None,
        sort: Sequence[str] | None = None,
        page: int = 1,
        size: int = 20,
    ) -> tuple[Sequence[ModelT], int]:
        """Returns (items, total) so the API layer can build any page format."""
        total = await self.repository.count(filters=filters)
        items = await self.repository.list(
            filters=filters, sort=sort, offset=(page - 1) * size, limit=size
        )
        return items, total

    # ------------------------------------------------------------- write
    async def create(self, schema: CreateSchemaT) -> ModelT:
        data = await self.before_create(schema.model_dump())
        obj = await self.repository.create(data)
        await self.session.commit()
        return obj

    async def update(
        self, id_: Any, schema: UpdateSchemaT | CreateSchemaT, *, partial: bool = True
    ) -> ModelT:
        """partial=True -> PATCH (only fields the client sent). False -> PUT (all fields)."""
        obj = await self.get(id_)
        data = schema.model_dump(exclude_unset=partial)
        if not data:
            return obj  # nothing to change, no DB round trip
        data = await self.before_update(obj, data)
        obj = await self.repository.update(obj, data)
        await self.session.commit()
        return obj

    async def delete(self, id_: Any) -> None:
        obj = await self.get(id_)
        await self.before_delete(obj)
        await self.repository.delete(obj)
        await self.session.commit()
