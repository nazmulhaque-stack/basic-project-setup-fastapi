"""Prometheus metrics instrumentation for FastAPI + asyncpg + Redis + Elasticsearch.

Exposes a /metrics endpoint via manual Counter/Histogram/Gauge registries for
the key service observability signals.

Metrics exposed:
  http_requests_total{method, path, status_code}           Counter
  http_request_duration_seconds{method, path}              Histogram
  db_pool_size_current                                     Gauge
  db_pool_checked_out                                      Gauge
  redis_operations_total{operation, status}                Counter
  redis_operation_duration_seconds{operation}              Histogram
  elasticsearch_operations_total{operation, index, status} Counter
  elasticsearch_operation_duration_seconds{operation}      Histogram
  cache_hits_total{prefix}                                 Counter
  cache_misses_total{prefix}                               Counter
"""

from __future__ import annotations

import logging
import time
from typing import Callable

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Graceful degradation when prometheus_client is not installed
# ---------------------------------------------------------------------------

try:
    from prometheus_client import (
        CONTENT_TYPE_LATEST,
        REGISTRY,
        Counter,
        Gauge,
        Histogram,
        generate_latest,
    )

    _PROMETHEUS_AVAILABLE = True
except ImportError:  # pragma: no cover
    _PROMETHEUS_AVAILABLE = False
    CONTENT_TYPE_LATEST = "text/plain; version=0.0.4; charset=utf-8"  # type: ignore[assignment]

    class _NoopMetric:  # type: ignore[no-redef]
        """Stub that silently absorbs all prometheus_client method calls."""

        def labels(self, **kwargs: object) -> "_NoopMetric":  # noqa: D401
            return self

        def inc(self, amount: float = 1) -> None:  # noqa: D401
            pass

        def set(self, value: float) -> None:  # noqa: D401
            pass

        def observe(self, amount: float) -> None:  # noqa: D401
            pass

    def Counter(name: str, documentation: str, labelnames=(), **kw) -> _NoopMetric:  # type: ignore[no-redef] # noqa: N802
        return _NoopMetric()

    def Gauge(name: str, documentation: str, labelnames=(), **kw) -> _NoopMetric:  # type: ignore[no-redef] # noqa: N802
        return _NoopMetric()

    def Histogram(name: str, documentation: str, labelnames=(), **kw) -> _NoopMetric:  # type: ignore[no-redef] # noqa: N802
        return _NoopMetric()

    def generate_latest() -> bytes:  # type: ignore[no-redef]
        return b"# prometheus_client not installed\n"

    class REGISTRY:  # type: ignore[no-redef]
        pass

    logger.warning(
        "prometheus_client is not installed — MetricsRegistry operates in no-op mode. "
        "Install it with: pip install prometheus-client"
    )

try:
    from fastapi import FastAPI, Request, Response
    from fastapi.responses import Response as FastAPIResponse
    from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
    from starlette.types import ASGIApp

    _FASTAPI_AVAILABLE = True
except ImportError:  # pragma: no cover
    _FASTAPI_AVAILABLE = False


# ---------------------------------------------------------------------------
# MetricsRegistry — singleton, module-level instance created at bottom
# ---------------------------------------------------------------------------

# Bucket boundaries tuned for a high-throughput API:
#   < 5 ms (cache hit), 10 ms, 25 ms, 50 ms, 100 ms, 250 ms,
#   500 ms (P99 target), 1 s, 2.5 s, 5 s, 10 s (circuit-breaker territory)
_HTTP_LATENCY_BUCKETS = (0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0)
_REDIS_LATENCY_BUCKETS = (0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5)
_ES_LATENCY_BUCKETS = (0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0)

# Paths excluded from HTTP instrumentation to avoid label cardinality noise
_SKIP_PATHS = frozenset({"/metrics", "/health", "/healthz", "/ready", "/liveness"})


class MetricsRegistry:
    """Centralised Prometheus metrics registry for the FastAPI service.

    Instantiate once at module load; use the singleton :data:`metrics` for
    recording.  All public ``record_*`` methods are safe to call from both
    sync and async contexts — they perform only in-process atomic increments.

    Example::

        from app.core.metrics import metrics

        metrics.record_cache_hit("products")
        metrics.record_redis_op("get", "hit", 0.002)
    """

    # ------------------------------------------------------------------
    # HTTP
    # ------------------------------------------------------------------
    http_requests_total: Counter = Counter(  # type: ignore[valid-type]
        "http_requests_total",
        "Total HTTP requests handled.",
        ["method", "path", "status_code"],
    )

    http_request_duration_seconds: Histogram = Histogram(  # type: ignore[valid-type]
        "http_request_duration_seconds",
        "HTTP request processing latency in seconds.",
        ["method", "path"],
        buckets=_HTTP_LATENCY_BUCKETS,
    )

    # ------------------------------------------------------------------
    # Database pool
    # ------------------------------------------------------------------
    db_pool_size_current: Gauge = Gauge(  # type: ignore[valid-type]
        "db_pool_size_current",
        "Current total connections in the asyncpg pool (acquired + idle).",
    )

    db_pool_checked_out: Gauge = Gauge(  # type: ignore[valid-type]
        "db_pool_checked_out",
        "Connections currently checked out of the asyncpg pool.",
    )

    # ------------------------------------------------------------------
    # Redis
    # ------------------------------------------------------------------
    redis_operations_total: Counter = Counter(  # type: ignore[valid-type]
        "redis_operations_total",
        "Total Redis operations by command and outcome.",
        ["operation", "status"],
    )

    redis_operation_duration_seconds: Histogram = Histogram(  # type: ignore[valid-type]
        "redis_operation_duration_seconds",
        "Redis command round-trip latency in seconds.",
        ["operation"],
        buckets=_REDIS_LATENCY_BUCKETS,
    )

    # ------------------------------------------------------------------
    # Elasticsearch
    # ------------------------------------------------------------------
    elasticsearch_operations_total: Counter = Counter(  # type: ignore[valid-type]
        "elasticsearch_operations_total",
        "Total Elasticsearch operations by command, index, and outcome.",
        ["operation", "index", "status"],
    )

    elasticsearch_operation_duration_seconds: Histogram = Histogram(  # type: ignore[valid-type]
        "elasticsearch_operation_duration_seconds",
        "Elasticsearch request latency in seconds.",
        ["operation"],
        buckets=_ES_LATENCY_BUCKETS,
    )


    # ------------------------------------------------------------------
    # Cache
    # ------------------------------------------------------------------
    cache_hits_total: Counter = Counter(  # type: ignore[valid-type]
        "cache_hits_total",
        "Total cache hits by cache key prefix.",
        ["prefix"],
    )

    cache_misses_total: Counter = Counter(  # type: ignore[valid-type]
        "cache_misses_total",
        "Total cache misses by cache key prefix.",
        ["prefix"],
    )

    # ------------------------------------------------------------------
    # Public recording helpers
    # ------------------------------------------------------------------

    def record_http_request(
        self,
        method: str,
        path: str,
        status_code: int,
        duration_seconds: float,
    ) -> None:
        """Record a completed HTTP request.

        Args:
            method:           HTTP method in uppercase (e.g. ``"GET"``).
            path:             Normalised URL path (e.g. ``"/api/v1/products"``).
            status_code:      HTTP response status code (e.g. ``200``).
            duration_seconds: Wall-clock request duration in seconds.
        """
        self.http_requests_total.labels(
            method=method,
            path=path,
            status_code=str(status_code),
        ).inc()
        self.http_request_duration_seconds.labels(
            method=method,
            path=path,
        ).observe(duration_seconds)

    def record_db_pool(self, pool_size: int, checked_out: int) -> None:
        """Update database connection pool gauge metrics.

        Args:
            pool_size:    Total connections (idle + acquired) in the pool.
            checked_out:  Connections currently in active use.
        """
        self.db_pool_size_current.set(pool_size)
        self.db_pool_checked_out.set(checked_out)

    def record_redis_op(
        self,
        operation: str,
        status: str,
        duration_seconds: float,
    ) -> None:
        """Record a Redis command execution.

        Args:
            operation:        Redis command name (e.g. ``"get"``, ``"set"``,
                              ``"evalsha"``).
            status:           Outcome label: ``"ok"`` or ``"error"``.
            duration_seconds: Command round-trip duration in seconds.
        """
        self.redis_operations_total.labels(operation=operation, status=status).inc()
        self.redis_operation_duration_seconds.labels(operation=operation).observe(duration_seconds)

    def record_es_op(
        self,
        operation: str,
        index: str,
        status: str,
        duration_seconds: float,
    ) -> None:
        """Record an Elasticsearch API call.

        Args:
            operation:        ES operation name (e.g. ``"index"``, ``"search"``,
                              ``"delete"``).
            index:            Target index name or alias.
            status:           Outcome label: ``"ok"`` or ``"error"``.
            duration_seconds: Request latency in seconds.
        """
        self.elasticsearch_operations_total.labels(
            operation=operation,
            index=index,
            status=status,
        ).inc()
        self.elasticsearch_operation_duration_seconds.labels(
            operation=operation,
        ).observe(duration_seconds)


    def record_cache_hit(self, prefix: str) -> None:
        """Increment the cache-hit counter for *prefix*.

        Args:
            prefix: Cache key prefix / entity namespace (e.g. ``"products"``).
        """
        self.cache_hits_total.labels(prefix=prefix).inc()

    def record_cache_miss(self, prefix: str) -> None:
        """Increment the cache-miss counter for *prefix*.

        Args:
            prefix: Cache key prefix / entity namespace (e.g. ``"products"``).
        """
        self.cache_misses_total.labels(prefix=prefix).inc()


# Module-level singleton — import and use directly:
#   from app.core.metrics import metrics
metrics: MetricsRegistry = MetricsRegistry()


# ---------------------------------------------------------------------------
# Starlette / FastAPI middleware
# ---------------------------------------------------------------------------

if _FASTAPI_AVAILABLE:

    class PrometheusMiddleware(BaseHTTPMiddleware):
        """ASGI middleware that records HTTP request count and latency.

        Skips ``/metrics`` and ``/health`` paths (and related liveness/
        readiness variants) to avoid polluting cardinality with internal
        probes.

        Add to your application::

            app.add_middleware(PrometheusMiddleware)
        """

        def __init__(self, app: ASGIApp) -> None:
            super().__init__(app)

        async def dispatch(
            self,
            request: Request,
            call_next: RequestResponseEndpoint,
        ) -> Response:
            path = request.url.path
            if path in _SKIP_PATHS:
                return await call_next(request)

            method = request.method.upper()
            start_time = time.perf_counter()
            status_code = 500  # default if an unhandled exception propagates

            try:
                response: Response = await call_next(request)
                status_code = response.status_code
                return response
            finally:
                duration = time.perf_counter() - start_time
                metrics.record_http_request(
                    method=method,
                    path=path,
                    status_code=status_code,
                    duration_seconds=duration,
                )

else:  # pragma: no cover

    class PrometheusMiddleware:  # type: ignore[no-redef]
        """No-op middleware stub when FastAPI is not installed."""

        def __init__(self, app: object) -> None:
            self.app = app


# ---------------------------------------------------------------------------
# /metrics endpoint
# ---------------------------------------------------------------------------


def metrics_endpoint() -> "FastAPIResponse":
    """FastAPI route handler that exposes the Prometheus text exposition format.

    Register with::

        app.add_api_route("/metrics", metrics_endpoint, methods=["GET"], include_in_schema=False)

    or use :func:`setup_metrics` which does this automatically.

    Returns:
        A :class:`~fastapi.Response` with the Prometheus scrape payload and the
        correct ``Content-Type`` header.
    """
    if not _FASTAPI_AVAILABLE:  # pragma: no cover
        raise RuntimeError("FastAPI is required to serve the /metrics endpoint.")

    output: bytes = generate_latest()  # type: ignore[call-arg]
    return FastAPIResponse(content=output, media_type=CONTENT_TYPE_LATEST)


# ---------------------------------------------------------------------------
# Setup helper
# ---------------------------------------------------------------------------


def setup_metrics(app: "FastAPI") -> None:
    """Attach Prometheus instrumentation to a FastAPI application.

    Registers :class:`PrometheusMiddleware` and mounts the ``GET /metrics``
    endpoint.  Call this once during application startup, typically inside
    ``create_app()`` or the application factory.

    Args:
        app: The :class:`~fastapi.FastAPI` application instance.

    Example::

        from fastapi import FastAPI
        from app.core.metrics import setup_metrics

        app = FastAPI()
        setup_metrics(app)
    """
    if not _FASTAPI_AVAILABLE:  # pragma: no cover
        logger.warning("FastAPI is not installed; skipping Prometheus middleware and endpoint setup.")
        return

    app.add_middleware(PrometheusMiddleware)
    app.add_api_route(
        "/metrics",
        metrics_endpoint,
        methods=["GET"],
        include_in_schema=False,
        tags=["Observability"],
    )
    logger.info("Prometheus metrics instrumentation registered on GET /metrics")


__all__ = [
    "MetricsRegistry",
    "PrometheusMiddleware",
    "metrics",
    "metrics_endpoint",
    "setup_metrics",
]
