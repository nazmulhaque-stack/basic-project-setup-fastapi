from pydantic import Field

from app.schemas.base import InputSchema, ReadSchema, make_partial


class CategoryCreate(InputSchema):
    name: str = Field(min_length=2, max_length=100, examples=["Smartphones"])
    description: str | None = Field(default=None, max_length=2000)
    is_active: bool = True


# PATCH schema generated from the create schema: same rules, every field optional.
CategoryUpdate = make_partial(CategoryCreate, name="CategoryUpdate")


class CategoryRead(ReadSchema):
    name: str
    description: str | None
    is_active: bool
