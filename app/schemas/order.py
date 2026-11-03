"""Order Pydantic schemas (example DTOs for the examples/ package)."""

from datetime import datetime
from typing import Optional
import uuid

from pydantic import BaseModel, ConfigDict, Field


class OrderCreate(BaseModel):
    """Payload for creating a new order."""

    customer_id: uuid.UUID
    amount_cents: int = Field(..., ge=0, description="Order total in cents")
    description: str = Field(default="", max_length=1024)
    status: str = Field(default="pending", max_length=64)


class OrderUpdate(BaseModel):
    """Payload for partial order updates."""

    status: Optional[str] = Field(default=None, max_length=64)
    amount_cents: Optional[int] = Field(default=None, ge=0)
    description: Optional[str] = Field(default=None, max_length=1024)


class OrderRead(BaseModel):
    """Public representation of an order."""

    id: uuid.UUID
    customer_id: uuid.UUID
    status: str
    amount_cents: int
    description: str
    is_active: bool
    created_at: datetime
    updated_at: Optional[datetime] = None

    model_config = ConfigDict(from_attributes=True)


__all__ = ["OrderCreate", "OrderUpdate", "OrderRead"]
