from decimal import Decimal

from pydantic import Field, field_validator

from app.schemas.base import InputSchema, ReadSchema, make_partial
from app.schemas.category import CategoryRead


class ProductCreate(InputSchema):
    name: str = Field(min_length=2, max_length=150, examples=["Galaxy A55"])
    sku: str = Field(min_length=3, max_length=64, examples=["SAM-A55-128"])
    price: Decimal = Field(gt=0, max_digits=12, decimal_places=2, examples=["38999.00"])
    stock: int = Field(default=0, ge=0)
    is_active: bool = True
    category_id: int = Field(gt=0)

    @field_validator("sku")
    @classmethod
    def normalize_sku(cls, value: str) -> str:
        return value.upper()


ProductUpdate = make_partial(ProductCreate, name="ProductUpdate")


class ProductRead(ReadSchema):
    name: str
    sku: str
    price: Decimal
    stock: int
    is_active: bool
    category_id: int


class ProductDetail(ProductRead):
    """Used only where the repository eager-loads the category."""

    category: CategoryRead
