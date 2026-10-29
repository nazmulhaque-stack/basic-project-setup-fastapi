"""Pydantic schemas package."""

from app.schemas.base import (
    BaseSchema,
    ErrorDetail,
    ErrorResponse,
    InputSchema,
    Page,
    PageResponse,
    ReadSchema,
    decode_cursor,
    encode_cursor,
    make_partial,
)
from app.schemas.category import CategoryCreate, CategoryRead, CategoryUpdate
from app.schemas.product import (
    ProductCreate,
    ProductDetail,
    ProductPage,
    ProductRead,
    ProductUpdate,
    StockAdjust,
)
from app.schemas.user import UserCreate, UserPage, UserRead, UserUpdate

__all__ = [
    "BaseSchema",
    "InputSchema",
    "ReadSchema",
    "Page",
    "make_partial",
    "ErrorDetail",
    "ErrorResponse",
    "PageResponse",
    "encode_cursor",
    "decode_cursor",
    "UserCreate",
    "UserUpdate",
    "UserRead",
    "UserPage",
    "ProductCreate",
    "ProductUpdate",
    "ProductRead",
    "ProductDetail",
    "ProductPage",
    "StockAdjust",
    "CategoryCreate",
    "CategoryUpdate",
    "CategoryRead",
]

