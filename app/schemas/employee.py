"""Pydantic v2 schemas for Employee entity."""

from datetime import datetime
from typing import Optional, Union
import uuid

from pydantic import BaseModel, ConfigDict, Field

from app.schemas.base import InputSchema, ReadSchema, make_partial


class EmployeeBase(BaseModel):
    """Shared employee properties."""

    name: str = Field(..., min_length=1, max_length=255, description="Full name of employee", examples=["John Doe"])
    age: int = Field(..., ge=18, le=120, description="Age of employee (18-120)", examples=[30])
    location: str = Field(..., min_length=1, max_length=255, description="Office or geographic location", examples=["New York"])
    user_id: uuid.UUID = Field(..., description="Unique 1-to-1 foreign key to User table")


class EmployeeCreate(EmployeeBase, InputSchema):
    """Payload for employee creation."""
    pass


# PATCH schema generated from create schema: all fields optional
EmployeeUpdate = make_partial(EmployeeCreate, name="EmployeeUpdate")


class EmployeeRead(ReadSchema):
    """Public safe representation of an employee."""

    id: uuid.UUID
    name: str
    age: int
    location: str
    user_id: uuid.UUID

    model_config = ConfigDict(from_attributes=True)


class EmployeePage(BaseModel):
    """Cursor-paginated container for employees."""

    items: list[EmployeeRead]
    next_cursor: Optional[str] = Field(
        default=None,
        description="Opaque base64 cursor token to retrieve the next contiguous page",
    )
    limit: int = Field(default=50, ge=1, le=100)


__all__ = [
    "EmployeeBase",
    "EmployeeCreate",
    "EmployeeUpdate",
    "EmployeeRead",
    "EmployeePage",
]
