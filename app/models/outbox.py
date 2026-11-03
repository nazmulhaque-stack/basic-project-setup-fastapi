"""Outbox event row dataclass — see app/services/outbox.py for the dispatcher."""

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Optional
import uuid

from app.models.base import Base


@dataclass
class OutboxEvent(Base):
    id: uuid.UUID
    topic: str
    event_key: str
    event_type: str
    payload: Dict[str, Any]
    created_at: Optional[datetime] = None
    published_at: Optional[datetime] = None
    attempts: int = 0
    last_error: Optional[str] = None


__all__ = ["OutboxEvent"]
