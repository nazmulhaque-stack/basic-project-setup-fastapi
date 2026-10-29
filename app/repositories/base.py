"""Generic Asynchronous Repository for SQLAlchemy 2.0.

Merges the dynamic OOP repository pattern with:
- AbstractRepository / IBaseRepository interface contracts
- Automatic model class inference via __init_subclass__
- Rich lookup operators (eq, ne, gt, gte, lt, lte, in, not_in, contains, icontains, startswith, endswith, isnull)
- Single & multi-column sorting with tie-breaker on PK
- Offset pagination (list, count) and High-Performance Keyset (cursor) pagination (list_keyset)
- Protected fields and safe _flush() with IntegrityError -> ConflictError translation
- Repositories never commit transactions; they only flush.
"""

from collections.abc import Callable, Mapping, Sequence
from datetime import datetime
import logging
from typing import (
    Any,
    ClassVar,
    Dict,
    Generic,
    List,
    Optional,
    Tuple,
    Type,
    TypeVar,
    Union,
    get_args,
    get_origin,
)

from pydantic import BaseModel
from sqlalchemy import (
    ColumnElement,
    Select,
    and_,
    delete,
    func,
    inspect,
    or_,
    select,
)
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import InstrumentedAttribute

from app.core.exceptions import ConflictError, InvalidQueryError
from app.db.base import Base
from app.repositories.interface import IBaseRepository

logger = logging.getLogger(__name__)

ModelType = TypeVar("ModelType", bound=Base)
IdType = TypeVar("IdType")
ModelT = ModelType

LOOKUP_SEP = "__"

# Operator dispatch map with autoescape on text operators
FILTER_OPERATORS: Dict[str, Callable[[InstrumentedAttribute[Any], Any], ColumnElement[bool]]] = {
    "eq": lambda col, val: col == val,
    "ne": lambda col, val: col != val,
    "gt": lambda col, val: col > val,
    "gte": lambda col, val: col >= val,
    "lt": lambda col, val: col < val,
    "lte": lambda col, val: col <= val,
    "in": lambda col, val: col.in_(val),
    "notin": lambda col, val: ~col.in_(val),
    "not_in": lambda col, val: ~col.in_(val),
    "like": lambda col, val: col.like(val),
    "ilike": lambda col, val: col.ilike(val),
    "contains": lambda col, val: col.contains(val, autoescape=True),
    "icontains": lambda col, val: col.icontains(val, autoescape=True),
    "startswith": lambda col, val: col.startswith(val, autoescape=True),
    "endswith": lambda col, val: col.endswith(val, autoescape=True),
    "isnull": lambda col, val: col.is_(None) if val else col.isnot(None),
}
OPERATOR_MAP = FILTER_OPERATORS


def split_lookup(key: str) -> Tuple[str, str]:
    """Splits 'price__gte' -> ('price', 'gte'); 'name' -> ('name', 'eq')."""
    field, _, op = key.partition(LOOKUP_SEP)
    return field, op or "eq"


class BaseRepository(IBaseRepository[ModelType, Any], Generic[ModelType, IdType]):
    """Generic async SQLAlchemy 2.0 repository implementing dynamic query, filter, and pagination engines."""

    model: ClassVar[Type[Base]]
    protected_fields: ClassVar[frozenset[str]] = frozenset({"id", "created_at", "updated_at"})
    default_sort: ClassVar[Tuple[str, ...]] = ("id",)

    def __init_subclass__(cls, **kwargs: Any) -> None:
        """Infers `model` from generic argument e.g. `CategoryRepository(SQLAlchemyRepository[Category])`."""
        super().__init_subclass__(**kwargs)
        if "model" in cls.__dict__:
            return
        for base in getattr(cls, "__orig_bases__", ()):
            origin = get_origin(base)
            if origin is not None and issubclass(origin, BaseRepository):
                args = get_args(base)
                if args and isinstance(args[0], type) and issubclass(args[0], Base):
                    cls.model = args[0]
                    return


    def __init__(
        self,
        model: Optional[Type[ModelType]] = None,
        session: Optional[AsyncSession] = None,
        db: Optional[AsyncSession] = None,
    ) -> None:
        active_session: Optional[AsyncSession] = session if session is not None else db
        if active_session is None:
            raise ValueError("An AsyncSession must be provided to Repository")

        self._session: AsyncSession = active_session

        # Resolve model from constructor or inferred class attribute
        resolved_model: Optional[Type[ModelType]] = model or getattr(self, "model", None)
        if resolved_model is None:
            raise TypeError(f"{type(self).__name__} must define `model` attribute or pass model to __init__.")
        self._model: Type[ModelType] = resolved_model

        mapper = inspect(self._model)
        self._columns: frozenset[str] = frozenset(attr.key for attr in mapper.column_attrs)
        self._pk = mapper.primary_key[0]

    @property
    def model(self) -> Type[ModelType]:
        return self._model

    @property
    def db(self) -> AsyncSession:
        return self._session

    @property
    def session(self) -> AsyncSession:
        return self._session

    # ------------------------------------------------------------------ helpers
    def _column(self, name: str) -> InstrumentedAttribute[Any]:
        if name not in self._columns:
            raise InvalidQueryError(f"Unknown field '{name}' for {self._model.__name__}.")
        return getattr(self._model, name)

    def _build_conditions(self, filters: Optional[Mapping[str, Any]]) -> List[ColumnElement[bool]]:
        conditions: List[ColumnElement[bool]] = []
        for key, value in (filters or {}).items():
            if value is None and not key.endswith("__isnull"):
                continue
            field, op = split_lookup(key)
            if op not in FILTER_OPERATORS:
                raise InvalidQueryError(f"Unknown operator '{op}' in '{key}'.")
            conditions.append(FILTER_OPERATORS[op](self._column(field), value))
        return conditions

    def _apply_filters(self, query: Any, **filters: Any) -> Any:
        clauses = self._build_conditions(filters)
        if clauses:
            query = query.where(and_(*clauses))
        return query

    def _apply_sort(self, stmt: Select[Any], sort: Optional[Sequence[str]]) -> Select[Any]:
        """sort=['-price', 'name'] -> ORDER BY price DESC, name ASC, id ASC."""
        keys = list(sort or self.default_sort)
        order_by: List[Any] = []
        for key in keys:
            desc = key.startswith("-")
            clean_key = key.lstrip("-+")
            if clean_key in self._columns:
                col = self._column(clean_key)
                order_by.append(col.desc() if desc else col.asc())
        # Tie-breaker on PK preserves deterministic pagination
        pk_key = self._pk.key
        if pk_key not in {k.lstrip("-+") for k in keys}:
            order_by.append(self._pk.asc())
        return stmt.order_by(*order_by)

    def _apply_ordering(
        self,
        query: Any,
        order_by: Union[str, Sequence[str], None] = None,
        descending: bool = False,
    ) -> Any:
        """Applies dynamic ordering based on column names and descending flags."""
        if not order_by:
            return query
        cols = [order_by] if isinstance(order_by, str) else list(order_by)
        sort_keys: List[str] = []
        for col in cols:
            if descending and not col.startswith("-"):
                sort_keys.append(f"-{col}")
            else:
                sort_keys.append(col)
        return self._apply_sort(query, sort_keys)

    def _clean(self, data: Mapping[str, Any]) -> Dict[str, Any]:
        unknown = set(data) - self._columns
        if unknown:
            raise InvalidQueryError(f"Unknown field(s) for {self._model.__name__}: {sorted(unknown)}")
        return {k: v for k, v in data.items() if k not in self.protected_fields}

    async def _flush(self, *objs: ModelType) -> None:
        """Flushes session and reloads server-assigned attributes."""
        try:
            await self._session.flush()
        except IntegrityError as exc:
            logger.warning("Integrity error on %s: %s", self._model.__name__, getattr(exc, "orig", exc))
            raise ConflictError(
                f"{self._model.__name__} violates a unique or foreign-key constraint."
            ) from exc
        for obj in objs:
            await self._session.refresh(obj)

    async def flush(self) -> None:
        await self._session.flush()

    # --------------------------------------------------------------------- read
    async def get_by_id(
        self,
        obj_id: Any,
        options: Optional[Sequence[Any]] = None,
    ) -> Optional[ModelType]:
        if options:
            stmt = select(self._model).where(self._pk == obj_id).options(*options)
            return (await self._session.scalars(stmt)).first()
        return await self._session.get(self._model, obj_id)

    async def get(
        self,
        entity_id: Any,
        options: Optional[Sequence[Any]] = None,
    ) -> Optional[ModelType]:
        return await self.get_by_id(entity_id, options=options)

    async def get_by(
        self,
        options: Optional[Sequence[Any]] = None,
        **filters: Any,
    ) -> Optional[ModelType]:
        query = select(self._model).where(*self._build_conditions(filters)).limit(1)
        if options:
            query = query.options(*options)
        return (await self._session.scalars(query)).first()

    async def get_all(
        self,
        limit: int = 100,
        offset: int = 0,
        options: Optional[Sequence[Any]] = None,
        **filters: Any,
    ) -> List[ModelType]:
        stmt = select(self._model).where(*self._build_conditions(filters))
        if options:
            stmt = stmt.options(*options)
        safe_offset: int = max(0, offset)
        safe_limit: int = min(100, max(1, limit))
        stmt = stmt.offset(safe_offset).limit(safe_limit)
        return list((await self._session.scalars(stmt)).all())

    async def list(
        self,
        offset: int = 0,
        limit: Optional[int] = 100,
        order_by: Union[str, Sequence[str], None] = None,
        descending: bool = False,
        options: Optional[Sequence[Any]] = None,
        sort: Optional[Sequence[str]] = None,
        filters: Optional[Mapping[str, Any]] = None,
        **extra_filters: Any,
    ) -> Sequence[ModelType]:
        """Dynamic offset pagination supporting both sort and order_by interfaces."""
        merged_filters: Dict[str, Any] = dict(filters or {})
        merged_filters.update(extra_filters)

        stmt = select(self._model).where(*self._build_conditions(merged_filters))
        if options:
            stmt = stmt.options(*options)

        # Resolve sort parameters
        effective_sort: List[str] = []
        if sort:
            effective_sort = list(sort)
        elif order_by:
            cols = [order_by] if isinstance(order_by, str) else list(order_by)
            for c in cols:
                effective_sort.append(f"-{c}" if descending and not c.startswith("-") else c)

        stmt = self._apply_sort(stmt, effective_sort or None).offset(max(0, offset))
        if limit is not None:
            stmt = stmt.limit(min(100, max(1, limit)))
        return (await self._session.scalars(stmt)).all()

    async def count(self, filters: Optional[Mapping[str, Any]] = None, **extra_filters: Any) -> int:
        merged = dict(filters or {})
        merged.update(extra_filters)
        stmt = select(func.count()).select_from(self._model).where(*self._build_conditions(merged))
        return int(await self._session.scalar(stmt) or 0)

    async def exists(self, **filters: Any) -> bool:
        stmt = select(self._pk).where(*self._build_conditions(filters)).limit(1)
        return (await self._session.scalar(stmt)) is not None

    async def exists_by(self, **kwargs: Any) -> bool:
        return await self.exists(**kwargs)

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
        """Dynamic keyset (cursor) pagination using composite seek condition."""
        order_attr = getattr(self._model, order_col)
        id_attr = getattr(self._model, id_col)

        safe_limit: int = min(100, max(1, limit))
        query = select(self._model).where(*self._build_conditions(filters))
        query = query.order_by(order_attr.asc(), id_attr.asc()).limit(safe_limit + 1)
        if options:
            query = query.options(*options)

        if cursor_dt is not None and cursor_id is not None:
            query = query.where(
                or_(
                    order_attr > cursor_dt,
                    and_(order_attr == cursor_dt, id_attr > cursor_id),
                )
            )

        rows = list((await self._session.scalars(query)).all())
        has_next = len(rows) > safe_limit
        return (rows[:safe_limit] if has_next else rows), has_next

    async def aggregate(
        self,
        func_name: str,
        column_name: str,
        **filters: Any,
    ) -> Any:
        col = getattr(self._model, column_name)
        sql_func = getattr(func, func_name.lower())
        query = select(sql_func(col)).select_from(self._model).where(*self._build_conditions(filters))
        return await self._session.scalar(query)

    # -------------------------------------------------------------------- write
    async def create(
        self,
        data: Union[BaseModel, Mapping[str, Any], ModelType],
        refresh: bool = False,
    ) -> ModelType:
        if isinstance(data, self._model):
            obj = data
        elif isinstance(data, BaseModel):
            obj = self._model(**data.model_dump())
        elif isinstance(data, Mapping):
            obj = self._model(**self._clean(data))
        else:
            obj = data

        self._session.add(obj)
        await self._flush(obj)
        return obj

    async def create_many(
        self,
        items: Sequence[Union[BaseModel, Mapping[str, Any], ModelType]],
    ) -> Sequence[ModelType]:
        models: List[ModelType] = []
        for item in items:
            if isinstance(item, self._model):
                models.append(item)
            elif isinstance(item, BaseModel):
                models.append(self._model(**item.model_dump()))
            elif isinstance(item, Mapping):
                models.append(self._model(**self._clean(item)))
            else:
                models.append(item)

        self._session.add_all(models)
        await self._flush(*models)
        return models

    async def bulk_create(
        self,
        items: Sequence[Union[BaseModel, Mapping[str, Any], ModelType]],
    ) -> Sequence[ModelType]:
        return await self.create_many(items)

    async def update(
        self,
        entity: ModelType,
        data: Optional[Union[BaseModel, Mapping[str, Any]]] = None,
        **kwargs: Any,
    ) -> ModelType:
        payload: Dict[str, Any] = {}
        if isinstance(data, BaseModel):
            payload = data.model_dump(exclude_unset=True)
        elif isinstance(data, Mapping):
            payload = dict(data)
        payload.update(kwargs)

        cleaned = self._clean(payload)
        for key, value in cleaned.items():
            setattr(entity, key, value)

        await self._flush(entity)
        return entity

    async def delete(self, entity: ModelType) -> None:
        await self._session.delete(entity)
        await self._session.flush()

    async def delete_by_id(self, entity_id: Any) -> bool:
        obj = await self.get(entity_id)
        if obj is None:
            return False
        await self.delete(obj)
        return True

    async def delete_where(self, **filters: Any) -> int:
        conditions = self._build_conditions(filters)
        if not conditions:
            raise InvalidQueryError("Refusing to delete_where without filters.")
        result = await self._session.execute(delete(self._model).where(*conditions))
        await self._session.flush()
        return int(result.rowcount or 0)


class SQLAlchemyRepository(BaseRepository[ModelType, Any], Generic[ModelType]):
    """Generic async SQLAlchemy 2.x implementation accepting single type argument: SQLAlchemyRepository[Model]."""

    def __init__(
        self,
        session: Optional[AsyncSession] = None,
        model: Optional[Type[ModelType]] = None,
        db: Optional[AsyncSession] = None,
    ) -> None:
        super().__init__(model=model, session=session, db=db)


AbstractRepository = IBaseRepository

__all__ = [
    "BaseRepository",
    "SQLAlchemyRepository",
    "AbstractRepository",
    "FILTER_OPERATORS",
    "OPERATOR_MAP",
    "LOOKUP_SEP",
    "split_lookup",
    "ModelT",
    "ModelType",
    "IdType",
]
