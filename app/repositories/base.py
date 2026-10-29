"""Generic repository layer.

AbstractRepository  -> the contract (what services depend on: Dependency Inversion).
SQLAlchemyRepository -> one generic implementation that gives every model full CRUD,
                        dynamic filtering (`field__op`), sorting and pagination.

A concrete repository is usually one line:

    class ProductRepository(SQLAlchemyRepository[Product]):
        pass                      # model is inferred from the generic argument

Rules:
  * Repositories never commit. They `flush()` so the DB assigns ids and checks
    constraints; the service decides when the transaction ends.
  * Repositories receive plain dicts, not API schemas, so they do not depend on the
    API layer and can be reused from scripts, workers, tests.
"""

import logging
from abc import ABC, abstractmethod
from collections.abc import Callable, Mapping, Sequence
from typing import Any, ClassVar, Generic, TypeVar, get_args, get_origin

from sqlalchemy import ColumnElement, Select, delete, func, inspect, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import InstrumentedAttribute
from sqlalchemy.sql.base import ExecutableOption

from app.core.exceptions import ConflictError, InvalidQueryError
from app.models.base import Base

ModelT = TypeVar("ModelT", bound=Base)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Filter operators: "price__gte=10" -> Product.price >= 10
# Extend this table to add operators everywhere at once (Open/Closed principle).
# ---------------------------------------------------------------------------
FILTER_OPERATORS: dict[str, Callable[[InstrumentedAttribute[Any], Any], ColumnElement[bool]]] = {
    "eq": lambda col, v: col == v,
    "ne": lambda col, v: col != v,
    "gt": lambda col, v: col > v,
    "gte": lambda col, v: col >= v,
    "lt": lambda col, v: col < v,
    "lte": lambda col, v: col <= v,
    "in": lambda col, v: col.in_(v),
    "not_in": lambda col, v: col.not_in(v),
    # autoescape=True: a user typing "%" or "_" searches for that literal character.
    "contains": lambda col, v: col.contains(v, autoescape=True),
    "icontains": lambda col, v: col.icontains(v, autoescape=True),
    "startswith": lambda col, v: col.startswith(v, autoescape=True),
    "isnull": lambda col, v: col.is_(None) if v else col.is_not(None),
}
LOOKUP_SEP = "__"


def split_lookup(key: str) -> tuple[str, str]:
    """'price__gte' -> ('price', 'gte'); 'name' -> ('name', 'eq')."""
    field, _, op = key.partition(LOOKUP_SEP)
    return field, op or "eq"


class AbstractRepository(ABC, Generic[ModelT]):
    """The contract every repository fulfils. Services type-hint against this."""

    @abstractmethod
    async def get(self, id_: Any, *, options: Sequence[ExecutableOption] = ()) -> ModelT | None: ...

    @abstractmethod
    async def get_by(self, **filters: Any) -> ModelT | None: ...

    @abstractmethod
    async def list(
        self,
        *,
        filters: Mapping[str, Any] | None = None,
        sort: Sequence[str] | None = None,
        offset: int = 0,
        limit: int | None = 100,
        options: Sequence[ExecutableOption] = (),
    ) -> Sequence[ModelT]: ...

    @abstractmethod
    async def count(self, *, filters: Mapping[str, Any] | None = None) -> int: ...

    @abstractmethod
    async def exists(self, **filters: Any) -> bool: ...

    @abstractmethod
    async def create(self, data: Mapping[str, Any]) -> ModelT: ...

    @abstractmethod
    async def create_many(self, rows: Sequence[Mapping[str, Any]]) -> Sequence[ModelT]: ...

    @abstractmethod
    async def update(self, obj: ModelT, data: Mapping[str, Any]) -> ModelT: ...

    @abstractmethod
    async def delete(self, obj: ModelT) -> None: ...

    @abstractmethod
    async def delete_where(self, **filters: Any) -> int: ...


class SQLAlchemyRepository(AbstractRepository[ModelT]):
    """Generic async SQLAlchemy 2.x implementation of AbstractRepository."""

    model: ClassVar[type[Base]]
    # Columns a caller is never allowed to write through create/update.
    protected_fields: ClassVar[frozenset[str]] = frozenset({"id", "created_at", "updated_at"})
    # Default ORDER BY when the caller gives none (stable pagination needs one).
    default_sort: ClassVar[tuple[str, ...]] = ("id",)

    def __init_subclass__(cls, **kwargs: Any) -> None:
        """Infer `model` from `SQLAlchemyRepository[Product]` if not set explicitly."""
        super().__init_subclass__(**kwargs)
        if "model" in cls.__dict__:
            return
        for base in getattr(cls, "__orig_bases__", ()):
            if get_origin(base) is not None and issubclass(get_origin(base), SQLAlchemyRepository):
                args = get_args(base)
                if args and isinstance(args[0], type) and issubclass(args[0], Base):
                    cls.model = args[0]
                    return

    def __init__(self, session: AsyncSession) -> None:
        if not hasattr(self, "model"):
            raise TypeError(f"{type(self).__name__} must define `model` or use Repo[Model].")
        self.session = session
        mapper = inspect(self.model)
        self._columns: frozenset[str] = frozenset(attr.key for attr in mapper.column_attrs)
        self._pk = mapper.primary_key[0]

    # ------------------------------------------------------------------ helpers
    def _column(self, name: str) -> InstrumentedAttribute[Any]:
        if name not in self._columns:
            raise InvalidQueryError(f"Unknown field '{name}' for {self.model.__name__}.")
        return getattr(self.model, name)  # type: ignore[no-any-return]

    def _build_conditions(self, filters: Mapping[str, Any] | None) -> list[ColumnElement[bool]]:
        conditions: list[ColumnElement[bool]] = []
        for key, value in (filters or {}).items():
            if value is None:  # "not provided" -> no condition. Use field__isnull for NULLs.
                continue
            field, op = split_lookup(key)
            if op not in FILTER_OPERATORS:
                raise InvalidQueryError(f"Unknown operator '{op}' in '{key}'.")
            conditions.append(FILTER_OPERATORS[op](self._column(field), value))
        return conditions

    def _apply_sort(self, stmt: Select[Any], sort: Sequence[str] | None) -> Select[Any]:
        """sort=['-price', 'name'] -> ORDER BY price DESC, name ASC, id ASC."""
        keys = list(sort or self.default_sort)
        order_by = []
        for key in keys:
            desc = key.startswith("-")
            col = self._column(key.lstrip("-+"))
            order_by.append(col.desc() if desc else col.asc())
        # Tie-breaker on the PK keeps pagination deterministic.
        if self._pk.key not in {k.lstrip("-+") for k in keys}:
            order_by.append(self._pk.asc())
        return stmt.order_by(*order_by)

    def _clean(self, data: Mapping[str, Any]) -> dict[str, Any]:
        unknown = set(data) - self._columns
        if unknown:
            raise InvalidQueryError(f"Unknown field(s) for {self.model.__name__}: {sorted(unknown)}")
        return {k: v for k, v in data.items() if k not in self.protected_fields}

    async def _flush(self, *objs: ModelT) -> None:
        """Flush + reload server-generated values (ids, defaults, onupdate timestamps)."""
        try:
            await self.session.flush()
        except IntegrityError as exc:
            await self.session.rollback()
            # Log the raw DB error; never send it to the client (leaks schema details).
            logger.warning("Integrity error on %s: %s", self.model.__name__, exc.orig)
            raise ConflictError(
                f"{self.model.__name__} violates a unique or foreign-key constraint."
            ) from exc
        for obj in objs:
            await self.session.refresh(obj)

    # --------------------------------------------------------------------- read
    async def get(self, id_: Any, *, options: Sequence[ExecutableOption] = ()) -> ModelT | None:
        if not options:
            return await self.session.get(self.model, id_)  # type: ignore[return-value]
        stmt = select(self.model).where(self._pk == id_).options(*options)
        return (await self.session.scalars(stmt)).first()  # type: ignore[return-value]

    async def get_by(self, **filters: Any) -> ModelT | None:
        stmt = select(self.model).where(*self._build_conditions(filters)).limit(1)
        return (await self.session.scalars(stmt)).first()  # type: ignore[return-value]

    async def list(
        self,
        *,
        filters: Mapping[str, Any] | None = None,
        sort: Sequence[str] | None = None,
        offset: int = 0,
        limit: int | None = 100,
        options: Sequence[ExecutableOption] = (),
    ) -> Sequence[ModelT]:
        stmt = select(self.model).where(*self._build_conditions(filters)).options(*options)
        stmt = self._apply_sort(stmt, sort).offset(offset)
        if limit is not None:
            stmt = stmt.limit(limit)
        return (await self.session.scalars(stmt)).all()  # type: ignore[return-value]

    async def count(self, *, filters: Mapping[str, Any] | None = None) -> int:
        stmt = select(func.count()).select_from(self.model).where(*self._build_conditions(filters))
        return int(await self.session.scalar(stmt) or 0)

    async def exists(self, **filters: Any) -> bool:
        stmt = select(self._pk).where(*self._build_conditions(filters)).limit(1)
        return (await self.session.scalar(stmt)) is not None

    # -------------------------------------------------------------------- write
    async def create(self, data: Mapping[str, Any]) -> ModelT:
        obj = self.model(**self._clean(data))
        self.session.add(obj)
        await self._flush(obj)  # type: ignore[arg-type]
        return obj  # type: ignore[return-value]

    async def create_many(self, rows: Sequence[Mapping[str, Any]]) -> Sequence[ModelT]:
        objs = [self.model(**self._clean(r)) for r in rows]
        self.session.add_all(objs)
        await self._flush(*objs)  # type: ignore[arg-type]
        return objs  # type: ignore[return-value]

    async def update(self, obj: ModelT, data: Mapping[str, Any]) -> ModelT:
        for key, value in self._clean(data).items():
            setattr(obj, key, value)
        await self._flush(obj)
        return obj

    async def delete(self, obj: ModelT) -> None:
        await self.session.delete(obj)
        await self._flush()

    async def delete_where(self, **filters: Any) -> int:
        conditions = self._build_conditions(filters)
        if not conditions:
            raise InvalidQueryError("Refusing to bulk-delete without filters.")
        result = await self.session.execute(delete(self.model).where(*conditions))
        return int(result.rowcount or 0)  # type: ignore[attr-defined]
