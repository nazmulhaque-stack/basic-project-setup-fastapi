"""Shared Pydantic v2 building blocks: base schemas, generic Page[T], partial-model factory."""

import copy
import math
from collections.abc import Sequence
from datetime import UTC, datetime
from typing import Any, Generic, TypeVar

from pydantic import BaseModel, ConfigDict, Field, create_model, field_validator

T = TypeVar("T")
SchemaT = TypeVar("SchemaT", bound=BaseModel)


class BaseSchema(BaseModel):
    """Base for every schema. `from_attributes` lets Pydantic read SQLAlchemy objects."""

    model_config = ConfigDict(from_attributes=True, str_strip_whitespace=True)


class InputSchema(BaseSchema):
    """Base for request bodies: unknown fields are rejected, not silently dropped."""

    model_config = ConfigDict(extra="forbid")


class ReadSchema(BaseSchema):
    """Base for responses of models that use IdMixin + TimestampMixin."""

    id: int
    created_at: datetime
    updated_at: datetime

    @field_validator("created_at", "updated_at")
    @classmethod
    def _assume_utc(cls, value: datetime) -> datetime:
        """DB stores UTC without tz info; label it so JSON says ...Z, not local time."""
        return value.replace(tzinfo=UTC) if value.tzinfo is None else value


class Page(BaseModel, Generic[T]):
    """Generic paginated response: Page[ProductRead], Page[CategoryRead], ..."""

    items: list[T]
    total: int = Field(ge=0)
    page: int = Field(ge=1)
    size: int = Field(ge=1)
    pages: int = Field(ge=0)

    @classmethod
    def create(cls, items: Sequence[Any], total: int, page: int, size: int) -> "Page[T]":
        return cls.model_validate(
            {
                "items": list(items),
                "total": total,
                "page": page,
                "size": size,
                "pages": math.ceil(total / size) if size else 0,
            },
            from_attributes=True,
        )


def make_partial(schema: type[SchemaT], *, name: str | None = None) -> type[SchemaT]:
    """Build a PATCH schema from a create schema, dynamically.

    Every field becomes optional (default None) but KEEPS its type and constraints:
      * omitted field         -> not in model_dump(exclude_unset=True) -> untouched
      * {"name": "x"}         -> validated with the original rules (min_length, gt, ...)
      * {"name": null}        -> 422 if the original field was not nullable
    Pydantic does not validate defaults, so `default=None` is safe for non-nullable types.
    Subclassing `schema` keeps its config (extra="forbid") and its field validators.
    """
    fields: dict[str, Any] = {}
    for field_name, field_info in schema.model_fields.items():
        new_info = copy.deepcopy(field_info)
        new_info.default = None
        new_info.default_factory = None
        fields[field_name] = (field_info.annotation, new_info)

    return create_model(  # type: ignore[call-overload,no-any-return]
        name or f"{schema.__name__}Partial",
        __base__=schema,
        __module__=schema.__module__,
        **fields,
    )
