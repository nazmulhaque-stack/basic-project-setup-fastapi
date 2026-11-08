"""Transactional Outbox Domain Dataclass Model.

Tracks changes before Elasticsearch synchronization.
No ORM, pure dataclass.
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Optional
import uuid


@dataclass
class OutboxEvent:
    """Outbox record representing a change to be synced to Elasticsearch."""

    id: uuid.UUID
    aggregate_type: str
    aggregate_id: str
    event_type: str
    payload: Dict[str, Any]
    status: str = "PENDING"
    retry_count: int = 0
    last_error: Optional[str] = None
    created_at: Optional[datetime] = None
    published_at: Optional[datetime] = None


__all__ = ["OutboxEvent"]
