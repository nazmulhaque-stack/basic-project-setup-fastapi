from datetime import datetime
from typing import Any, List, Optional, Union
import uuid

from pydantic import BaseModel, ConfigDict, Field, field_validator

from app.schemas.base import InputSchema
from app.schemas.category import CategoryRead


class ProductBase(BaseModel):
    """Shared product attributes."""
    name: str = Field(..., min_length=1, max_length=255, description="Product name")
    sku: str = Field(..., min_length=1, max_length=100, description="Unique stock keeping unit")
    price_cents: int = Field(..., ge=0, description="Price in cents (e.g. 1999 = $19.99)")
    category: str = Field(default="general", min_length=1, max_length=100, description="Product category")
    category_id: Optional[int] = Field(default=None, description="Optional foreign key to categories table")
    is_active: bool = Field(default=True, description="Whether product is active for purchase")
    stock: int = Field(default=0, ge=0, description="Physical on-hand inventory units")

    @field_validator("sku")
    @classmethod
    def normalize_sku(cls, value: str) -> str:
        return value.upper().strip()


class ProductCreate(ProductBase):
    """Payload for creating a new product."""
    pass


class ProductUpdate(BaseModel):
    """Payload for partial product updates."""
    name: Optional[str] = Field(default=None, min_length=1, max_length=255)
    sku: Optional[str] = Field(default=None, min_length=1, max_length=100)
    price_cents: Optional[int] = Field(default=None, ge=0)
    category: Optional[str] = Field(default=None, min_length=1, max_length=100)
    category_id: Optional[int] = Field(default=None)
    is_active: Optional[bool] = Field(default=None)
    stock: Optional[int] = Field(default=None, ge=0)

    @field_validator("sku")
    @classmethod
    def normalize_sku(cls, value: Optional[str]) -> Optional[str]:
        return value.upper().strip() if value is not None else None


class ProductRead(BaseModel):
    """Public representation of a product."""
    id: Union[uuid.UUID, int]
    name: str
    sku: str
    price_cents: int
    category: str
    category_id: Optional[int] = None
    is_active: bool
    stock: int = Field(default=0, description="Physical on-hand inventory units")
    reserved_quantity: int = Field(default=0, description="Units held by active reservations")
    available_quantity: int = Field(default=0, description="On-hand units not currently reserved")
    created_at: datetime
    updated_at: Optional[datetime] = None

    model_config = ConfigDict(from_attributes=True)

    @field_validator("stock", mode="before")
    @classmethod
    def coerce_stock(cls, v: Any) -> int:
        return 0 if v is None else v

    @field_validator("reserved_quantity", mode="before")
    @classmethod
    def coerce_reserved_quantity(cls, v: Any) -> int:
        return 0 if v is None else v


class ProductDetail(ProductRead):
    """Product representation eager-loading full category details."""
    category_rel: Optional[CategoryRead] = None


class StockAdjust(InputSchema):
    """Payload for atomic stock inventory adjustments."""
    delta: int = Field(..., description="Quantity delta to adjust stock (+ to restock, - to sell)")


class ProductPage(BaseModel):
    """Cursor-paginated container for products."""
    items: List[ProductRead]
    next_cursor: Optional[str] = Field(
        default=None,
        description="Opaque base64 cursor token to retrieve the next contiguous page",
    )
    limit: int = Field(default=50, ge=1, le=100)


__all__ = [
    "ProductBase",
    "ProductCreate",
    "ProductUpdate",
    "ProductRead",
    "ProductDetail",
    "StockAdjust",
    "ProductPage",
]
