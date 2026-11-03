"""Order row dataclass (example entity for the examples/ package)."""

from dataclasses import dataclass
from datetime import datetime
from typing import Optional
import uuid

from app.models.base import Base


@dataclass
class Order(Base):
    """Plain dataclass representing a row in the ``orders`` table."""

    id: uuid.UUID
    customer_id: uuid.UUID
    status: str = "pending"
    amount_cents: int = 0
    description: str = ""
    is_active: bool = True
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    def __repr__(self) -> str:
        return f"<Order id={self.id} status={self.status} amount_cents={self.amount_cents}>"


__all__ = ["Order"]
