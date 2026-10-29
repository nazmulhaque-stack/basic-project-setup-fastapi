"""Protected API endpoints for centralized Elasticsearch application logs and aggregations."""

from datetime import datetime
from typing import Annotated, Optional

from fastapi import APIRouter, Depends, Query, status
from fastapi.responses import ORJSONResponse

from app.api.deps import LogSearchServiceDep
from app.schemas.log import (
    LogAggregationResponse,
    LogEntryRead,
    LogQueryFilter,
    LogServicesResponse,
)

router = APIRouter(prefix="/logs", tags=["Logs & Observability"])


@router.get(
    "",
    response_model=dict,
    status_code=status.HTTP_200_OK,
    summary="Search application logs with filters and pagination",
)
async def search_logs(
    service: LogSearchServiceDep,
    level: Optional[str] = Query(None, description="Filter by level (DEBUG, INFO, WARNING, ERROR, CRITICAL)"),
    app_service: Optional[str] = Query(None, alias="service", description="Filter by microservice name"),
    environment: Optional[str] = Query(None, description="Filter by environment (production, staging, etc.)"),
    request_id: Optional[str] = Query(None, description="Exact lookup by request ID"),
    trace_id: Optional[str] = Query(None, description="Exact lookup by distributed trace ID"),
    tenant_id: Optional[str] = Query(None, description="Filter by tenant ID"),
    search: Optional[str] = Query(None, max_length=200, description="Full-text search query on log message"),
    start_time: Optional[datetime] = Query(None, description="Start timestamp filter (ISO-8601)"),
    end_time: Optional[datetime] = Query(None, description="End timestamp filter (ISO-8601)"),
    limit: Annotated[int, Query(ge=1, le=500)] = 50,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> ORJSONResponse:
    """Searches application logs with deterministic ordering and bounded parameters."""
    filters = LogQueryFilter(
        level=level,
        service=app_service,
        environment=environment,
        request_id=request_id,
        trace_id=trace_id,
        tenant_id=tenant_id,
        search=search,
        start_time=start_time,
        end_time=end_time,
        limit=limit,
        offset=offset,
    )

    items, total = await service.search_logs(filters)
    return ORJSONResponse(
        content={
            "items": [item.model_dump(by_alias=True, mode="json") for item in items],
            "total": total,
            "limit": limit,
            "offset": offset,
        }
    )


@router.get(
    "/errors",
    response_model=dict,
    status_code=status.HTTP_200_OK,
    summary="Search error and critical application logs",
)
async def get_errors(
    service: LogSearchServiceDep,
    app_service: Optional[str] = Query(None, alias="service", description="Filter by microservice name"),
    environment: Optional[str] = Query(None, description="Filter by environment"),
    request_id: Optional[str] = Query(None, description="Filter by request ID"),
    search: Optional[str] = Query(None, max_length=200, description="Full-text query"),
    limit: Annotated[int, Query(ge=1, le=500)] = 50,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> ORJSONResponse:
    """Convenience endpoint returning application error logs."""
    filters = LogQueryFilter(
        service=app_service,
        environment=environment,
        request_id=request_id,
        search=search,
        limit=limit,
        offset=offset,
    )
    items, total = await service.get_errors(filters)
    return ORJSONResponse(
        content={
            "items": [item.model_dump(by_alias=True, mode="json") for item in items],
            "total": total,
            "limit": limit,
            "offset": offset,
        }
    )


@router.get(
    "/aggregations",
    response_model=LogAggregationResponse,
    status_code=status.HTTP_200_OK,
    summary="Compute faceted aggregations across application logs",
)
async def get_log_aggregations(
    service: LogSearchServiceDep,
    app_service: Optional[str] = Query(None, alias="service", description="Filter by microservice name"),
    environment: Optional[str] = Query(None, description="Filter by environment"),
    start_time: Optional[datetime] = Query(None, description="Start timestamp (ISO-8601)"),
    end_time: Optional[datetime] = Query(None, description="End timestamp (ISO-8601)"),
    interval: Annotated[str, Query(pattern=r"^(1m|5m|15m|30m|1h|6h|12h|1d)$")] = "1h",
) -> LogAggregationResponse:
    """Returns faceted metrics across log levels, services, routes, and timeline."""
    filters = LogQueryFilter(
        service=app_service,
        environment=environment,
        start_time=start_time,
        end_time=end_time,
    )
    return await service.aggregate_logs(filters, interval=interval)


@router.get(
    "/services",
    response_model=LogServicesResponse,
    status_code=status.HTTP_200_OK,
    summary="List distinct services emitting application logs",
)
async def get_emitting_services(service: LogSearchServiceDep) -> LogServicesResponse:
    """Returns list of all distinct service names discovered in log index."""
    return await service.get_services()


__all__ = ["router"]
