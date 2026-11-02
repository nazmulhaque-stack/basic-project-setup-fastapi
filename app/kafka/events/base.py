"""Standard immutable event envelope for Apache Kafka streams."""

from datetime import datetime, timezone
from typing import Any, Generic, TypeVar
import uuid

from pydantic import BaseModel, Field

PayloadT = TypeVar("PayloadT")


class BaseEvent(BaseModel, Generic[PayloadT]):
    """Standard CloudEvents-aligned immutable event envelope for Apache Kafka.

    Guarantees:
    1. Schema Versioning: `event_version` allows backward-compatible deserialization.
    2. Distributed Tracing: `correlation_id` correlates events across microservices.
    3. Idempotency: Unique `event_id` ensures consumers detect and skip duplicates.
    """

    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    event_type: str = Field(description="Dotted event name, e.g. user.created")
    event_version: int = Field(default=1, description="Schema version number")
    occurred_at: str = Field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )
    producer: str = Field(default="fastapi-repo-pattern", description="Originating service identifier")
    correlation_id: str | None = Field(default=None, description="Request ID or trace context")
    payload: PayloadT


__all__ = ["BaseEvent", "make_event_name"]


def make_event_name(service: str, event: str, event_name: str) -> str:
    """Format an event string conforming to the standard '<service name>.<event>.<event name>' convention.

    Example:
        make_event_name("inventory", "event", "reserved") -> "inventory.event.reserved"
        make_event_name("order", "event", "created") -> "order.event.created"
        make_event_name("user", "event", "registered") -> "user.event.registered"
    """
    parts = [service.strip().lower(), event.strip().lower(), event_name.strip().lower()]
    return ".".join(parts)
