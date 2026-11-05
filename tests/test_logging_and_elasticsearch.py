"""Comprehensive tests for structured logging, asynchronous buffer, and Elasticsearch log search & aggregations."""

import asyncio
from datetime import datetime, timedelta, timezone
import logging
from typing import Any, Dict, List
from unittest.mock import AsyncMock, MagicMock
import orjson
import pytest
from httpx import ASGITransport, AsyncClient

from app.core.elasticsearch import AsyncElasticsearchService, NullElasticsearch
from app.core.logging import (
    AsyncLogBuffer,
    StructuredLogFormatter,
    get_log_context,
    sanitize_value,
    set_log_context,
)
from app.main import app


# ==============================================================================
# Structured Logging & Sanitization Tests
# ==============================================================================


def test_structured_log_formatter_format() -> None:
    """Verifies that StructuredLogFormatter generates valid JSON with all required fields."""
    formatter = StructuredLogFormatter(service="test-service", environment="testing")
    set_log_context(request_id="req-12345", trace_id="trace-67890", tenant_id="tenant-acme")

    logger = logging.getLogger("test.logger")
    record = logger.makeRecord(
        name="test.logger",
        level=logging.INFO,
        fn="test_file.py",
        lno=42,
        msg="User authenticated successfully",
        args=(),
        exc_info=None,
    )
    record.http_method = "POST"
    record.route = "/api/v1/login"
    record.status_code = 200
    record.duration_ms = 12.34

    output = formatter.format(record)
    data = orjson.loads(output)

    assert "@timestamp" in data
    assert data["level"] == "INFO"
    assert data["service"] == "test-service"
    assert data["environment"] == "testing"
    assert data["logger"] == "test.logger"
    assert data["message"] == "User authenticated successfully"
    assert data["request_id"] == "req-12345"
    assert data["trace_id"] == "trace-67890"
    assert data["tenant_id"] == "tenant-acme"
    assert data["http_method"] == "POST"
    assert data["route"] == "/api/v1/login"
    assert data["status_code"] == 200
    assert data["duration_ms"] == 12.34


def test_log_sanitization_redacts_credentials() -> None:
    """Verifies that sensitive credentials, tokens, and secrets are sanitized."""
    raw_payload = {
        "username": "alice",
        "password": "SuperSecretPassword123!",
        "auth_token": "bearer eyJhbGciOi...",
        "api_key": "live_key_9999",
        "profile": {
            "credit_card": "411111111111",
            "bio": "Software Engineer",
        },
    }

    sanitized = sanitize_value(raw_payload)
    assert sanitized["username"] == "alice"
    assert sanitized["password"] == "[REDACTED]"
    assert sanitized["auth_token"] == "[REDACTED]"
    assert sanitized["api_key"] == "[REDACTED]"
    assert sanitized["profile"]["credit_card"] == "[REDACTED]"
    assert sanitized["profile"]["bio"] == "Software Engineer"


@pytest.mark.asyncio
async def test_coroutine_context_isolation() -> None:
    """Verifies ContextVar isolation across concurrent asynchronous tasks."""
    async def task_a():
        set_log_context(request_id="id-A", trace_id="trace-A")
        await asyncio.sleep(0.01)
        ctx = get_log_context()
        assert ctx["request_id"] == "id-A"
        assert ctx["trace_id"] == "trace-A"

    async def task_b():
        set_log_context(request_id="id-B", trace_id="trace-B")
        await asyncio.sleep(0.01)
        ctx = get_log_context()
        assert ctx["request_id"] == "id-B"
        assert ctx["trace_id"] == "trace-B"

    await asyncio.gather(task_a(), task_b())


# ==============================================================================
# AsyncLogBuffer Tests
# ==============================================================================


@pytest.mark.asyncio
async def test_async_log_buffer_batching_and_flush() -> None:
    """Verifies that AsyncLogBuffer enqueues and flushes batches to Elasticsearch client."""
    mock_es_client = AsyncMock()
    mock_es_client.bulk.return_value = {"errors": False, "items": []}

    buffer = AsyncLogBuffer(
        es_client=mock_es_client,
        log_index="test-logs",
        buffer_size=10,
        batch_size=5,
        flush_interval=0.1,
    )
    buffer.start()

    # Enqueue 3 items
    for i in range(3):
        enqueued = buffer.enqueue({"message": f"Log event {i}"})
        assert enqueued is True

    # Allow background flush task to drain queue
    await asyncio.sleep(0.2)
    await buffer.stop()

    assert mock_es_client.bulk.called
    assert buffer.total_logs_processed == 3
    assert buffer.dropped_logs_count == 0


@pytest.mark.asyncio
async def test_async_log_buffer_backpressure_drops_when_full() -> None:
    """Verifies backpressure handling when buffer is saturated."""
    mock_es_client = AsyncMock()
    buffer = AsyncLogBuffer(
        es_client=mock_es_client,
        log_index="test-logs",
        buffer_size=3,
        batch_size=10,
        flush_interval=10.0,  # Slow flush so queue fills up
    )
    buffer.start()

    # Fill queue to capacity (3)
    assert buffer.enqueue({"event": 1}) is True
    assert buffer.enqueue({"event": 2}) is True
    assert buffer.enqueue({"event": 3}) is True

    # 4th item must be dropped
    dropped = buffer.enqueue({"event": 4})
    assert dropped is False
    assert buffer.dropped_logs_count == 1

    await buffer.stop()


# ==============================================================================
# Health Probe Integration Tests
# ==============================================================================


@pytest.mark.asyncio
async def test_readyz_reports_components() -> None:
    """Verifies /readyz reports database, redis, elasticsearch, and log_buffer status."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/readyz")
        # Even if DB is mocked or unconfigured in tests, verify component keys are returned
        data = resp.json()
        assert "components" in data
        assert "database" in data["components"]
        assert "redis" in data["components"]
        assert "elasticsearch" in data["components"]
        assert "log_buffer" in data["components"]
