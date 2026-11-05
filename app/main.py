"""Stateless High-Performance FastAPI Application.

Architecture:
1. Raw asyncpg connection pool (PgBouncer transaction-pooling compatible).
2. Redis 7 caching and atomic Lua rate limiter.
3. AES-256-GCM AEAD encryption and HMAC-SHA256 blind indexing for PII.
4. Elasticsearch for full-text search.
5. FastAPI official documentation style, stateless architecture.
"""

from contextlib import asynccontextmanager
import logging
import sys
import time
from typing import Any, AsyncGenerator, Awaitable, Callable, Dict, List, Optional
import uuid

import asyncpg
from fastapi import Depends, FastAPI, HTTPException, Request, Response, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import ORJSONResponse
from fastapi.routing import APIRoute

from app.api.v1.health import router as health_router
from app.api.v1.router import api_v1_router
from app.core.config import get_settings
from app.core.elasticsearch import (
    AsyncElasticsearchService,
    IElasticsearchService,
    NullElasticsearch,
    create_elasticsearch_client,
)
from app.core.exceptions import AppException
from app.core.logging import (
    AsyncElasticsearchLogHandler,
    AsyncLogBuffer,
    StructuredLogFormatter,
    set_log_context,
    setup_log_index_template,
)
from app.core.metrics import setup_metrics
from app.core.redis import close_redis_client, create_redis_client
from app.core.security import CryptoService
from app.db.pool import close_db_pool, create_db_pool
from app.schemas.base import ErrorDetail, ErrorResponse
from app.services.redis_rate_limit import RATE_LIMIT_LUA_SCRIPT, rate_limiter

# Setup structured application logging
root_logger = logging.getLogger()
root_logger.setLevel(logging.INFO)
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(StructuredLogFormatter())
root_logger.addHandler(console_handler)
logger = logging.getLogger("app.main")


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """FastAPI lifespan context manager for startup and graceful shutdown."""
    settings = get_settings()
    logger.info("Initializing application resources for %s...", settings.APP_NAME)

    # 1. Initialize asyncpg connection pool
    db_pool = await create_db_pool(settings)
    app.state.db_pool = db_pool

    # 2. Initialize Redis Client
    redis_client = await create_redis_client(settings)
    app.state.redis = redis_client

    # 3. Register Atomic Rate Limiter Lua Script
    try:
        app.state.rate_limit_lua_sha = await redis_client.script_load(RATE_LIMIT_LUA_SCRIPT)
        logger.info("Registered rate limiter Lua script with SHA: %s", app.state.rate_limit_lua_sha)
    except Exception as exc:
        logger.warning("Could not pre-load rate limit Lua script: %s. Direct EVAL fallback enabled.", exc)
        app.state.rate_limit_lua_sha = ""

    # 4. Initialize AES-256-GCM CryptoService & Blind Indexer
    crypto_keys = settings.get_encryption_keys()
    blind_index_key = settings.get_blind_index_key()
    app.state.crypto_service = CryptoService(
        keys=crypto_keys,
        active_version=settings.ACTIVE_KEY_VERSION,
        blind_index_key=blind_index_key,
    )
    logger.info("Initialized CryptoService with active key version: %d", settings.ACTIVE_KEY_VERSION)

    # 5. Initialize Elasticsearch 8 Client
    es_client = create_elasticsearch_client(settings)
    es_service: IElasticsearchService
    log_buffer: Optional[AsyncLogBuffer] = None
    es_log_handler: Optional[AsyncElasticsearchLogHandler] = None

    if es_client is not None:
        full_log_index = f"{settings.ELASTICSEARCH_INDEX_PREFIX}_{settings.ELASTICSEARCH_LOG_INDEX}"
        es_service = AsyncElasticsearchService(
            client=es_client,
            index_prefix=settings.ELASTICSEARCH_INDEX_PREFIX,
        )
        log_buffer = AsyncLogBuffer(
            es_client=es_client,
            log_index=full_log_index,
            buffer_size=settings.ELASTICSEARCH_LOG_BUFFER_SIZE,
            batch_size=settings.ELASTICSEARCH_LOG_BATCH_SIZE,
            flush_interval=settings.ELASTICSEARCH_LOG_FLUSH_INTERVAL,
        )
        log_buffer.start()
        es_log_handler = AsyncElasticsearchLogHandler(
            buffer=log_buffer,
            formatter=StructuredLogFormatter(service=settings.APP_NAME),
        )
        root_logger.addHandler(es_log_handler)
        logger.info("Configured Elasticsearch client pointing to %s", settings.ELASTICSEARCH_URL)
    else:
        es_service = NullElasticsearch()

    app.state.elasticsearch = es_service
    app.state.log_buffer = log_buffer

    yield

    # Graceful Shutdown
    logger.info("Closing application resources...")

    if log_buffer is not None:
        try:
            await log_buffer.stop()
            if es_log_handler is not None:
                root_logger.removeHandler(es_log_handler)
        except Exception as exc:
            logger.warning("Error draining log buffer: %s", exc)

    try:
        if hasattr(app.state, "elasticsearch"):
            await app.state.elasticsearch.close()
    except Exception as exc:
        logger.warning("Error closing Elasticsearch client: %s", exc)

    try:
        await close_redis_client(app.state.redis)
    except Exception as exc:
        logger.warning("Error closing Redis client: %s", exc)

    try:
        await close_db_pool(app.state.db_pool)
    except Exception as exc:
        logger.warning("Error closing DB pool: %s", exc)

    logger.info("Worker shutdown complete.")


def operation_id(route: APIRoute) -> str:
    """Short, stable operationIds ('user_list') -> clean generated API clients."""
    return route.name


# Instantiate stateless FastAPI application with ORJSONResponse default
app = FastAPI(
    title="High-Performance Stateless FastAPI Service",
    description="Stateless high-performance API with raw asyncpg, AES-GCM encryption, Redis Cluster cache, Elasticsearch CQRS search, and PgBouncer pooling",
    version="2.0.0",
    default_response_class=ORJSONResponse,
    lifespan=lifespan,
    generate_unique_id_function=operation_id,
)


# ------------------------------------------------------------------------------
# Request ID & Tracing Middleware
# ------------------------------------------------------------------------------
@app.middleware("http")
async def request_id_and_tracing_middleware(
    request: Request,
    call_next: Callable[[Request], Awaitable[Response]],
) -> Response:
    """Assigns unique correlation ID, propagates trace context, and tracks latency."""
    start_time = time.perf_counter()
    req_id: str = request.headers.get("x-request-id") or str(uuid.uuid4())
    trace_id: str = request.headers.get("x-trace-id") or request.headers.get("traceparent") or str(uuid.uuid4())
    tenant_id: Optional[str] = request.headers.get("x-tenant-id")

    request.state.request_id = req_id
    request.state.trace_id = trace_id
    request.state.tenant_id = tenant_id

    set_log_context(request_id=req_id, trace_id=trace_id, tenant_id=tenant_id)

    response: Response = await call_next(request)
    duration_ms = round((time.perf_counter() - start_time) * 1000, 2)

    response.headers["X-Request-ID"] = req_id
    response.headers["X-Trace-ID"] = trace_id

    logger.info(
        "%s %s -> %d (%.2fms)",
        request.method,
        request.url.path,
        response.status_code,
        duration_ms,
        extra={
            "http_method": request.method,
            "route": request.url.path,
            "status_code": response.status_code,
            "duration_ms": duration_ms,
            "request_id": req_id,
            "trace_id": trace_id,
            "tenant_id": tenant_id,
        },
    )
    return response


# ------------------------------------------------------------------------------
# Global Exception Handlers (Standard Error Envelope, No Traceback Leaks)
# ------------------------------------------------------------------------------
def get_request_id(request: Request) -> str:
    return getattr(request.state, "request_id", str(uuid.uuid4()))


@app.exception_handler(AppException)
async def app_exception_handler(request: Request, exc: AppException) -> ORJSONResponse:
    """Handles explicit application domain exceptions (NotFoundError, ConflictError, etc.)."""
    headers: Dict[str, str] = {}
    if hasattr(exc, "retry_after"):
        headers["Retry-After"] = str(exc.retry_after)

    envelope: ErrorResponse = ErrorResponse(
        error=ErrorDetail(
            code=exc.code,
            message=exc.message,
            request_id=get_request_id(request),
        )
    )
    return ORJSONResponse(
        status_code=exc.status_code,
        content=envelope.model_dump(),
        headers=headers,
    )


@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException) -> ORJSONResponse:
    """Handles standard HTTP exceptions with uniform JSON envelope and custom headers."""
    envelope: ErrorResponse = ErrorResponse(
        error=ErrorDetail(
            code=f"HTTP_{exc.status_code}",
            message=str(exc.detail),
            request_id=get_request_id(request),
        )
    )
    return ORJSONResponse(
        status_code=exc.status_code,
        content=envelope.model_dump(),
        headers=exc.headers or {},
    )


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError) -> ORJSONResponse:
    """Formats validation errors cleanly without exposing internal details."""
    errors: List[Dict[str, Any]] = exc.errors()
    msg: str = errors[0].get("msg", "Validation error") if errors else "Invalid request payload"
    field: str = ".".join(str(loc) for loc in errors[0].get("loc", [])) if errors else ""
    full_message: str = f"{field}: {msg}" if field else msg

    envelope: ErrorResponse = ErrorResponse(
        error=ErrorDetail(
            code="VALIDATION_ERROR",
            message=full_message,
            request_id=get_request_id(request),
        )
    )
    return ORJSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        content=envelope.model_dump(),
    )


@app.exception_handler(asyncpg.PostgresError)
async def database_exception_handler(request: Request, exc: asyncpg.PostgresError) -> ORJSONResponse:
    """Sanitizes database errors so SQL statements and internal state never leak to client."""
    logger.error("Unhandled database error: %s", exc)
    envelope: ErrorResponse = ErrorResponse(
        error=ErrorDetail(
            code="DATABASE_ERROR",
            message="A database error occurred while processing your request.",
            request_id=get_request_id(request),
        )
    )
    return ORJSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content=envelope.model_dump(),
    )


@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception) -> ORJSONResponse:
    """Catch-all for unhandled exceptions ensuring stack traces are logged but never returned."""
    logger.exception("Unhandled server exception: %s", exc)
    envelope: ErrorResponse = ErrorResponse(
        error=ErrorDetail(
            code="INTERNAL_SERVER_ERROR",
            message="An unexpected internal server error occurred.",
            request_id=get_request_id(request),
        )
    )
    return ORJSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content=envelope.model_dump(),
    )


# Mount versioned API v1 router behind the Redis Lua rate limiter
_settings = get_settings()
app.include_router(
    api_v1_router,
    prefix="/api/v1",
    dependencies=[
        Depends(
            rate_limiter(
                limit=_settings.RATE_LIMIT_DEFAULT,
                window_seconds=_settings.RATE_LIMIT_WINDOW,
                scope="api",
            )
        )
    ],
)
app.include_router(health_router)
setup_metrics(app)
