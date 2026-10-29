"""Shared Pydantic v2 building blocks: base schemas, generic Page[T], partial-model factory, and cursor tokens."""

import base64
from collections.abc import Sequence
import copy
from datetime import UTC, datetime
import math
from typing import Any, Generic, List, Optional, Tuple, TypeVar
import uuid

from fastapi import HTTPException, status
from pydantic import BaseModel, ConfigDict, Field, create_model, field_validator

T = TypeVar("T")
SchemaT = TypeVar("SchemaT", bound=BaseModel)


# ------------------------------------------------------------------------------
# Core Pydantic v2 Base Schemas
# ------------------------------------------------------------------------------


class BaseSchema(BaseModel):
    """Base for every schema. `from_attributes` lets Pydantic read SQLAlchemy objects."""

    model_config = ConfigDict(from_attributes=True, str_strip_whitespace=True)


class InputSchema(BaseSchema):
    """Base for request bodies: unknown fields are rejected, not silently dropped."""

    model_config = ConfigDict(extra="forbid")


class ReadSchema(BaseSchema):
    """Base for responses of models that use IdMixin + TimestampMixin."""

    id: Any
    created_at: datetime
    updated_at: Optional[datetime] = None

    @field_validator("created_at", "updated_at", mode="before")
    @classmethod
    def _assume_utc(cls, value: Any) -> Any:
        """DB stores UTC without tz info; label it so JSON says ...Z, not local time."""
        if isinstance(value, datetime):
            return value.replace(tzinfo=UTC) if value.tzinfo is None else value
        return value


# ------------------------------------------------------------------------------
# Pagination Models: Offset Page[T] + Keyset PageResponse[T]
# ------------------------------------------------------------------------------


class Page(BaseModel, Generic[T]):
    """Generic offset-paginated response: Page[ProductRead], Page[UserRead], ..."""

    items: List[T]
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


class PageResponse(BaseModel, Generic[T]):
    """Generic cursor-paginated container for ultra-high-throughput keyset seeks."""

    items: List[T]
    next_cursor: Optional[str] = Field(
        default=None,
        description="Opaque base64 cursor token to retrieve the next contiguous page",
    )
    limit: int = Field(default=50, ge=1, le=100)


# ------------------------------------------------------------------------------
# Dynamic Partial Schema Factory (for PATCH endpoints)
# ------------------------------------------------------------------------------


def make_partial(schema: type[SchemaT], *, name: str | None = None) -> type[SchemaT]:
    """Build a PATCH schema from a create schema dynamically.

    Every field becomes optional (default None) but KEEPS its type and constraints:
      * omitted field         -> not in model_dump(exclude_unset=True) -> untouched
      * {"name": "x"}         -> validated with the original rules (min_length, gt, ...)
      * {"name": null}        -> 422 if the original field was not nullable
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


# ------------------------------------------------------------------------------
# Keyset Cursor Utilities
# ------------------------------------------------------------------------------


def encode_cursor(created_at: datetime, item_id: Any) -> str:
    """Encodes order timestamp/value and item ID into an opaque, URL-safe Base64 token."""
    raw: str = f"{created_at.isoformat()}|{item_id}"
    return base64.urlsafe_b64encode(raw.encode("utf-8")).decode("ascii").rstrip("=")


def decode_cursor(cursor_str: str) -> Tuple[datetime, uuid.UUID]:
    """Decodes opaque Base64 token back to (datetime, uuid)."""
    clean: str = cursor_str.strip()
    missing_padding: int = len(clean) % 4
    if missing_padding:
        clean += "=" * (4 - missing_padding)
    try:
        raw: str = base64.urlsafe_b64decode(clean.encode("ascii")).decode("utf-8")
        iso_ts: str
        uid_str: str
        iso_ts, uid_str = raw.split("|", 1)
        return datetime.fromisoformat(iso_ts), uuid.UUID(uid_str)
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid pagination cursor token: {exc}",
        ) from exc


# ------------------------------------------------------------------------------
# Error Envelopes
# ------------------------------------------------------------------------------


class ErrorDetail(BaseModel):
    """Uniform error detail structure."""

    code: str = Field(..., description="Machine-readable error classification code")
    message: str = Field(..., description="Human-readable explanation of the error")
    request_id: Optional[str] = Field(default=None, description="Unique correlation ID for tracing")


class ErrorResponse(BaseModel):
    """Standardized root error envelope."""

    error: ErrorDetail


__all__ = [
    "BaseSchema",
    "InputSchema",
    "ReadSchema",
    "Page",
    "PageResponse",
    "make_partial",
    "encode_cursor",
    "decode_cursor",
    "ErrorDetail",
    "ErrorResponse",
]
