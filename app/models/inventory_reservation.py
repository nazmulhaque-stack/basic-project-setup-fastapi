"""Persistent inventory reservation row dataclass, consumed from Kafka."""

from dataclasses import dataclass
from datetime import datetime
from typing import Optional
import uuid

from app.models.base import Base


@dataclass
class InventoryReservation(Base):
    id: uuid.UUID
    event_id: str
    reservation_id: str
    order_id: str
    product_id: uuid.UUID
    quantity: int
    status: str
    location_id: Optional[str] = None
    expires_at: Optional[datetime] = None
    created_at: Optional[datetime] = None


__all__ = ["InventoryReservation"]
