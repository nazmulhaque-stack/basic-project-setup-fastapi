"""User domain event schemas for Kafka streaming."""

from pydantic import BaseModel, Field

from app.kafka.events.base import BaseEvent


class UserCreatedPayload(BaseModel):
    user_id: str
    email: str
    name: str
    role: str
    phone_masked: str


class UserCreatedEvent(BaseEvent[UserCreatedPayload]):
    event_type: str = Field(default="user.created")


class UserUpdatedPayload(BaseModel):
    user_id: str
    email: str | None = None
    name: str | None = None
    role: str | None = None
    phone_masked: str | None = None


class UserUpdatedEvent(BaseEvent[UserUpdatedPayload]):
    event_type: str = Field(default="user.updated")


__all__ = [
    "UserCreatedPayload",
    "UserCreatedEvent",
    "UserUpdatedPayload",
    "UserUpdatedEvent",
]
