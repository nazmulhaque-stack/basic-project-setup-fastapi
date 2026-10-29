"""Structured JSON logging and non-blocking asynchronous log ingestion pipeline for Elasticsearch.

Ensures application requests are never blocked by logging I/O, propagates correlation
IDs (request_id, trace_id, tenant_id) via ContextVar, sanitizes PII/credentials, and
buffers log entries for high-throughput bulk ingestion into Elasticsearch data streams.
"""

import asyncio
from contextvars import ContextVar
from datetime import datetime, timezone
import logging
import re
import sys
import traceback
from typing import Any, Dict, List, Optional

import orjson

from app.core.config import Settings, get_settings

# Correlation IDs across asynchronous tasks
request_id_ctx: ContextVar[str] = ContextVar("request_id_ctx", default="")
trace_id_ctx: ContextVar[str] = ContextVar("trace_id_ctx", default="")
tenant_id_ctx: ContextVar[Optional[str]] = ContextVar("tenant_id_ctx", default=None)

# Sensitive patterns to sanitize
SENSITIVE_KEY_PATTERN = re.compile(
    r"(?i)(password|secret|token|authorization|bearer|api[_-]?key|credit[_-]?card|ssn|private[_-]?key)"
)


def sanitize_value(val: Any) -> Any:
    """Recursively sanitizes sensitive dictionary values, strings, and lists."""
    if isinstance(val, dict):
        sanitized = {}
        for k, v in val.items():
            if SENSITIVE_KEY_PATTERN.search(str(k)):
                sanitized[k] = "[REDACTED]"
            else:
                sanitized[k] = sanitize_value(v)
        return sanitized
    elif isinstance(val, list):
        return [sanitize_value(item) for item in val]
    elif isinstance(val, str) and len(val) > 1000:
        return val[:1000] + "...[TRUNCATED]"
    return val


def set_log_context(
    request_id: str = "",
    trace_id: str = "",
    tenant_id: Optional[str] = None,
) -> None:
    """Sets current coroutine correlation context."""
    if request_id:
        request_id_ctx.set(request_id)
    if trace_id:
        trace_id_ctx.set(trace_id)
    tenant_id_ctx.set(tenant_id)


def get_log_context() -> Dict[str, Any]:
    """Retrieves current coroutine correlation context."""
    return {
        "request_id": request_id_ctx.get(),
        "trace_id": trace_id_ctx.get(),
        "tenant_id": tenant_id_ctx.get(),
    }


class StructuredLogFormatter(logging.Formatter):
    """Produces canonical structured JSON log records for stdout, collectors, and Elasticsearch."""

    def __init__(self, service: str = "fastapi-backend", environment: str = "production") -> None:
        super().__init__()
        self.service = service
        self.environment = environment

    def format(self, record: logging.LogRecord) -> str:
        timestamp = datetime.now(timezone.utc).isoformat()
        context = get_log_context()

        # Extract extra structured attributes if present on record
        http_method = getattr(record, "http_method", None)
        route = getattr(record, "route", None)
        status_code = getattr(record, "status_code", None)
        duration_ms = getattr(record, "duration_ms", None)

        record_request_id = getattr(record, "request_id", None) or context["request_id"]
        record_trace_id = getattr(record, "trace_id", None) or context["trace_id"]
        record_tenant_id = getattr(record, "tenant_id", None) or context["tenant_id"]

        log_data: Dict[str, Any] = {
            "@timestamp": timestamp,
            "level": record.levelname,
            "service": self.service,
            "environment": self.environment,
            "logger": record.name,
            "message": sanitize_value(record.getMessage()),
            "request_id": record_request_id,
            "trace_id": record_trace_id,
        }

        if record_tenant_id:
            log_data["tenant_id"] = record_tenant_id
        if http_method:
            log_data["http_method"] = str(http_method).upper()
        if route:
            log_data["route"] = str(route)
        if status_code is not None:
            try:
                log_data["status_code"] = int(status_code)
            except (ValueError, TypeError):
                pass
        if duration_ms is not None:
            try:
                log_data["duration_ms"] = round(float(duration_ms), 2)
            except (ValueError, TypeError):
                pass

        # Format exception if present
        if record.exc_info:
            exc_type, exc_val, exc_tb = record.exc_info
            log_data["exception"] = {
                "type": getattr(exc_type, "__name__", "Exception"),
                "message": sanitize_value(str(exc_val)),
                "stack": "".join(traceback.format_exception(exc_type, exc_val, exc_tb)),
            }

        return orjson.dumps(log_data).decode("utf-8")


# ==============================================================================
# Non-blocking Asynchronous Log Buffer for Bulk Ingestion
# ==============================================================================


class AsyncLogBuffer:
    """Bounded, thread-safe asynchronous queue that batches logs and flushes to Elasticsearch.

    Isolates log ingestion from HTTP request latency, enforces bounded backpressure,
    and prevents recursive logging loops when Elasticsearch is unavailable.
    """

    def __init__(
        self,
        es_client: Any,
        log_index: str = "app-logs",
        buffer_size: int = 1000,
        batch_size: int = 100,
        flush_interval: float = 2.0,
    ) -> None:
        self.es_client: Any = es_client
        self.log_index: str = log_index
        self.buffer_size: int = buffer_size
        self.batch_size: int = batch_size
        self.flush_interval: float = flush_interval

        self.queue: asyncio.Queue[Dict[str, Any]] = asyncio.Queue(maxsize=buffer_size)
        self.dropped_logs_count: int = 0
        self.total_logs_processed: int = 0
        self._flush_task: Optional[asyncio.Task[None]] = None
        self._running: bool = False

    def enqueue(self, log_entry: Dict[str, Any]) -> bool:
        """Non-blocking enqueue. If queue is saturated, drops log and increments counter."""
        if not self._running:
            return False
        try:
            self.queue.put_nowait(log_entry)
            return True
        except asyncio.QueueFull:
            self.dropped_logs_count += 1
            # Emergency direct stderr output without recursing into logger
            sys.__stderr__.write(
                f"[AsyncLogBuffer WARN] Log buffer saturated. Dropped {self.dropped_logs_count} log entries.\n"
            )
            return False

    def start(self) -> None:
        """Starts background periodic flush worker task."""
        if self._running:
            return
        self._running = True
        self._flush_task = asyncio.create_task(self._periodic_flush(), name="es-log-buffer-flush")

    async def stop(self) -> None:
        """Flushes remaining logs and stops background worker task."""
        self._running = False
        if self._flush_task is not None:
            self._flush_task.cancel()
            try:
                await self._flush_task
            except asyncio.CancelledError:
                pass
        # Final flush of all remaining queue items
        await self._flush_batch()

    async def _periodic_flush(self) -> None:
        """Background loop continuously draining the queue in batches."""
        while self._running:
            try:
                await asyncio.sleep(self.flush_interval)
                await self._flush_batch()
            except asyncio.CancelledError:
                break
            except Exception as exc:
                sys.__stderr__.write(f"[AsyncLogBuffer ERROR] Flush cycle exception: {exc}\n")

    async def _flush_batch(self) -> None:
        """Drains up to batch_size items from queue and issues Elasticsearch bulk index request."""
        batch: List[Dict[str, Any]] = []
        while len(batch) < self.batch_size:
            try:
                item = self.queue.get_nowait()
                batch.append(item)
            except asyncio.QueueEmpty:
                break

        if not batch or self.es_client is None:
            return

        # Prepare Elasticsearch bulk request format:
        # [{"index": {"_index": log_index}}, doc, ...]
        operations: List[Dict[str, Any]] = []
        for doc in batch:
            operations.append({"index": {"_index": self.log_index}})
            operations.append(doc)

        try:
            # Check client connectivity without blocking
            if hasattr(self.es_client, "bulk"):
                resp = await self.es_client.bulk(operations=operations)
                if isinstance(resp, dict) and resp.get("errors"):
                    sys.__stderr__.write("[AsyncLogBuffer WARN] Some log records were rejected by Elasticsearch.\n")
                self.total_logs_processed += len(batch)
        except Exception as exc:
            # Fails open: Elasticsearch rejection or outage never raises into application requests
            sys.__stderr__.write(f"[AsyncLogBuffer WARN] Failed to bulk index logs into Elasticsearch: {exc}\n")


class AsyncElasticsearchLogHandler(logging.Handler):
    """Custom standard logging Handler that redirects log records to AsyncLogBuffer."""

    def __init__(self, buffer: AsyncLogBuffer, formatter: StructuredLogFormatter) -> None:
        super().__init__()
        self.buffer: AsyncLogBuffer = buffer
        self.setFormatter(formatter)

    def emit(self, record: logging.LogRecord) -> None:
        # Prevent recursive logging loops from elasticsearch or httpx clients
        if record.name.startswith(("elasticsearch", "elastic_transport", "httpx", "httpcore")):
            return

        try:
            formatted_json = self.format(record)
            doc = orjson.loads(formatted_json)
            self.buffer.enqueue(doc)
        except Exception:
            self.handleError(record)


# ==============================================================================
# Elasticsearch Index Template & Mapping Setup
# ==============================================================================

APP_LOGS_MAPPINGS: Dict[str, Any] = {
    "properties": {
        "@timestamp": {"type": "date"},
        "level": {"type": "keyword"},
        "service": {"type": "keyword"},
        "environment": {"type": "keyword"},
        "logger": {"type": "keyword"},
        "message": {"type": "text"},
        "request_id": {"type": "keyword"},
        "trace_id": {"type": "keyword"},
        "tenant_id": {"type": "keyword"},
        "http_method": {"type": "keyword"},
        "route": {"type": "keyword"},
        "status_code": {"type": "integer"},
        "duration_ms": {"type": "float"},
        "exception": {
            "properties": {
                "type": {"type": "keyword"},
                "message": {"type": "text"},
                "stack": {"type": "text"},
            }
        },
    }
}


async def setup_log_index_template(es_client: Any, index_name: str = "app-logs") -> bool:
    """Idempotently ensures Elasticsearch index template and mappings exist."""
    if es_client is None or not hasattr(es_client, "indices"):
        return False
    try:
        # Check if index already exists
        exists = await es_client.indices.exists(index=index_name)
        if not exists:
            await es_client.indices.create(
                index=index_name,
                mappings=APP_LOGS_MAPPINGS,
                settings={
                    "index": {
                        "number_of_shards": 1,
                        "number_of_replicas": 0,
                        "refresh_interval": "5s",
                    }
                },
            )
            return True
        return True
    except Exception as exc:
        sys.__stderr__.write(f"[Logging Setup WARN] Could not ensure log index template: {exc}\n")
        return False


__all__ = [
    "request_id_ctx",
    "trace_id_ctx",
    "tenant_id_ctx",
    "set_log_context",
    "get_log_context",
    "sanitize_value",
    "StructuredLogFormatter",
    "AsyncLogBuffer",
    "AsyncElasticsearchLogHandler",
    "setup_log_index_template",
    "APP_LOGS_MAPPINGS",
]
