"""Health check endpoints for load balancers and orchestrators."""

import logging
from typing import Any, Dict, Union
from fastapi import APIRouter, Request, status
from fastapi.responses import ORJSONResponse
from sqlalchemy import text

logger = logging.getLogger(__name__)

router = APIRouter(tags=["Health"])


@router.get(
    "/healthz",
    summary="Liveness Probe",
    status_code=status.HTTP_200_OK,
)
async def healthz() -> Dict[str, str]:
    """Liveness probe for Kubernetes / Load Balancer.

    Never queries dependencies to ensure O(1) instantaneous response.
    """
    return {"status": "healthy"}


@router.get(
    "/readyz",
    summary="Readiness Probe",
    status_code=status.HTTP_200_OK,
    response_model=None,
)
async def readyz(request: Request) -> Union[Dict[str, Any], ORJSONResponse]:
    """Readiness probe verifying DB, Redis, and Elasticsearch connectivity."""
    db_ok: bool = False
    redis_ok: bool = False
    es_status: str = "disabled"
    log_buffer_info: Dict[str, Any] = {"active": False}

    # 1. Check Database connection (Critical)
    try:
        session_factory = getattr(request.app.state, "session_factory", None)
        if session_factory is not None:
            async with session_factory() as session:
                await session.execute(text("SELECT 1"))
            db_ok = True
    except Exception as exc:
        logger.error("Readiness check DB error: %s", exc)

    # 2. Check Redis connection (Critical)
    try:
        redis_client = getattr(request.app.state, "redis", None)
        if redis_client is not None:
            await redis_client.ping()
            redis_ok = True
    except Exception as exc:
        logger.error("Readiness check Redis error: %s", exc)

    # 3. Check Elasticsearch (Non-blocking: degraded if offline, does not fail readiness)
    try:
        es = getattr(request.app.state, "elasticsearch", None)
        if es is not None and hasattr(es, "ping"):
            es_alive = await es.ping()
            es_status = "healthy" if es_alive else "degraded"
    except Exception:
        es_status = "degraded"

    # 4. Check Log Buffer metrics
    log_buffer = getattr(request.app.state, "log_buffer", None)
    if log_buffer is not None:
        log_buffer_info = {
            "active": True,
            "queue_size": log_buffer.queue.qsize(),
            "dropped_logs": log_buffer.dropped_logs_count,
            "total_processed": log_buffer.total_logs_processed,
        }

    components = {
        "database": db_ok,
        "redis": redis_ok,
        "elasticsearch": es_status,
        "log_buffer": log_buffer_info,
    }

    if not (db_ok and redis_ok):
        return ORJSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content={
                "status": "unhealthy",
                "components": components,
            },
        )

    return {
        "status": "ready",
        "components": components,
    }
