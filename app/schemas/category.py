"""Category schemas defining inputs and outputs with strict validation."""

from typing import Optional

from pydantic import Field

from app.schemas.base import InputSchema, ReadSchema, make_partial


class CategoryCreate(InputSchema):
    """Schema for creating a new category."""

    name: str = Field(min_length=2, max_length=100, examples=["Electronics"])
    description: Optional[str] = Field(default=None, max_length=2000)
    is_active: bool = True


# PATCH schema generated from the create schema: same validation rules, every field optional
CategoryUpdate = make_partial(CategoryCreate, name="CategoryUpdate")


class CategoryRead(ReadSchema):
    """Public read schema representing a category."""

    name: str
    description: Optional[str] = None
    is_active: bool = True


__all__ = ["CategoryCreate", "CategoryUpdate", "CategoryRead"]
