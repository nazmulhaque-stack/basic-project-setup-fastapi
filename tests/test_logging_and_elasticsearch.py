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
from app.schemas.log import LogQueryFilter
from app.services.log_search import LogSearchService


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
# LogSearchService & Elasticsearch Aggregations Tests
# ==============================================================================


@pytest.mark.asyncio
async def test_log_search_service_queries_and_filters() -> None:
    """Verifies LogSearchService query building, filtering, and result mapping."""
    mock_es_service = AsyncMock()
    sample_hit = {
        "_source": {
            "@timestamp": datetime.now(timezone.utc).isoformat(),
            "level": "ERROR",
            "service": "order-service",
            "environment": "production",
            "logger": "app.orders",
            "message": "Payment gateway timeout",
            "request_id": "req-999",
            "trace_id": "tr-888",
            "status_code": 504,
            "duration_ms": 5001.2,
        }
    }
    mock_es_service.search_raw.return_value = {
        "hits": {"total": {"value": 1}, "hits": [sample_hit]}
    }

    service = LogSearchService(es=mock_es_service, log_index="test-logs")
    filters = LogQueryFilter(
        level="ERROR",
        service="order-service",
        request_id="req-999",
        search="timeout",
        limit=20,
        offset=0,
    )

    items, total = await service.search_logs(filters)
    assert total == 1
    assert len(items) == 1
    assert items[0].level == "ERROR"
    assert items[0].service == "order-service"
    assert items[0].message == "Payment gateway timeout"

    # Verify query DSL arguments passed to search_raw
    call_args = mock_es_service.search_raw.call_args[0]
    index_arg, body_arg = call_args[0], call_args[1]
    assert index_arg == "test-logs"
    assert "bool" in body_arg["query"]
    assert any("order-service" in str(f) for f in body_arg["query"]["bool"]["filter"])


@pytest.mark.asyncio
async def test_log_search_service_aggregations() -> None:
    """Verifies faceted aggregation response calculation."""
    mock_es_service = AsyncMock()
    mock_es_service.search_raw.return_value = {
        "hits": {"total": {"value": 150}},
        "aggregations": {
            "by_level": {"buckets": [{"key": "INFO", "doc_count": 120}, {"key": "ERROR", "doc_count": 30}]},
            "by_service": {"buckets": [{"key": "api-gateway", "doc_count": 150}]},
            "by_route": {"buckets": [{"key": "/api/v1/products", "doc_count": 90}]},
            "by_status_code": {"buckets": [{"key": "200", "doc_count": 120}, {"key": "500", "doc_count": 30}]},
            "timeline": {"buckets": [{"key_as_string": "2026-10-29T12:00:00Z", "doc_count": 150}]},
        },
    }

    service = LogSearchService(es=mock_es_service, log_index="test-logs")
    filters = LogQueryFilter()
    aggs = await service.aggregate_logs(filters, interval="1h")

    assert aggs.total_logs == 150
    assert aggs.by_level["INFO"] == 120
    assert aggs.by_level["ERROR"] == 30
    assert aggs.by_service["api-gateway"] == 150
    assert aggs.by_route["/api/v1/products"] == 90
    assert len(aggs.timeline) == 1
    assert aggs.timeline[0].count == 150


@pytest.mark.asyncio
async def test_log_search_service_get_services() -> None:
    """Verifies unique services aggregation retrieval."""
    mock_es_service = AsyncMock()
    mock_es_service.search_raw.return_value = {
        "aggregations": {
            "unique_services": {
                "buckets": [{"key": "service-a"}, {"key": "service-b"}]
            }
        }
    }
    service = LogSearchService(es=mock_es_service)
    res = await service.get_services()
    assert res.services == ["service-a", "service-b"]


# ==============================================================================
# API Endpoints & Health Probe Integration Tests
# ==============================================================================


@pytest.mark.asyncio
async def test_log_api_endpoints() -> None:
    """Verifies /api/v1/logs endpoints with mock LogSearchService dependency override."""
    mock_log_service = AsyncMock()
    mock_log_service.search_logs.return_value = ([], 0)
    mock_log_service.get_errors.return_value = ([], 0)
    mock_log_service.get_services.return_value = MagicMock(services=["app-api", "worker"])
    mock_log_service.aggregate_logs.return_value = {
        "total_logs": 0,
        "by_level": {},
        "by_service": {},
        "by_route": {},
        "by_status_code": {},
        "timeline": [],
    }

    from app.api.deps import get_log_search_service
    app.dependency_overrides[get_log_search_service] = lambda: mock_log_service

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        # 1. Search logs
        r1 = await client.get("/api/v1/logs?level=INFO&limit=10")
        assert r1.status_code == 200
        assert r1.json()["total"] == 0

        # 2. Errors endpoint
        r2 = await client.get("/api/v1/logs/errors")
        assert r2.status_code == 200

        # 3. Aggregations endpoint
        r3 = await client.get("/api/v1/logs/aggregations")
        assert r3.status_code == 200

        # 4. Services endpoint
        r4 = await client.get("/api/v1/logs/services")
        assert r4.status_code == 200
        assert "app-api" in r4.json()["services"]

    app.dependency_overrides.clear()


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
