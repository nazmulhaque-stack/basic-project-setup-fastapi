"""Tests for Kafka event schemas and retry policy."""

import pytest

from app.kafka.events.base import BaseEvent
from app.kafka.events.user_events import UserCreatedEvent, UserCreatedPayload
from app.kafka.retry import RetryPolicy


def test_kafka_event_envelope():
    payload = UserCreatedPayload(
        user_id="user-123",
        email="test@example.com",
        name="Test",
        role="admin",
        phone_masked="+1*****4567",
    )
    event = UserCreatedEvent(payload=payload, correlation_id="req-1")
    data = event.model_dump(mode="json")
    assert data["event_type"] == "user.created"
    assert data["event_version"] == 1
    assert data["correlation_id"] == "req-1"
    assert "event_id" in data
    assert "occurred_at" in data


def test_kafka_retry_policy():
    policy = RetryPolicy(max_retries=3, base_backoff_seconds=1.0)
    assert policy.calculate_backoff(1) == 1.0
    assert policy.calculate_backoff(2) == 2.0
    assert policy.calculate_backoff(3) == 4.0
    assert policy.is_retryable(ValueError("invalid")) is False
    assert policy.is_retryable(ConnectionError("timeout")) is True
