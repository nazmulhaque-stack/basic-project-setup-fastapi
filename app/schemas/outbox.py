"""Outbox schema for monitoring and inspection."""

from datetime import datetime
from typing import Any, Dict, Optional
import uuid

from pydantic import BaseModel, ConfigDict


class OutboxEventRead(BaseModel):
    id: uuid.UUID
    aggregate_type: str
    aggregate_id: str
    event_type: str
    payload: Dict[str, Any]
    status: str
    retry_count: int
    last_error: Optional[str] = None
    created_at: Optional[datetime] = None
    published_at: Optional[datetime] = None

    model_config = ConfigDict(from_attributes=True)


__all__ = ["OutboxEventRead"]
