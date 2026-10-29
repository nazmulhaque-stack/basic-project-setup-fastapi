"""Pydantic schemas for centralized application logs, filters, and aggregations."""

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, ConfigDict, Field, field_validator


class LogExceptionDetail(BaseModel):
    """Sanitized exception information captured in structured logs."""

    type: str
    message: str
    stack: Optional[str] = None

    model_config = ConfigDict(from_attributes=True)


class LogEntryRead(BaseModel):
    """Canonical log document schema returned by log search endpoints."""

    timestamp: datetime = Field(alias="@timestamp")
    level: str
    service: str
    environment: str
    logger: str
    message: str
    request_id: Optional[str] = None
    trace_id: Optional[str] = None
    tenant_id: Optional[str] = None
    http_method: Optional[str] = None
    route: Optional[str] = None
    status_code: Optional[int] = None
    duration_ms: Optional[float] = None
    exception: Optional[LogExceptionDetail] = None

    model_config = ConfigDict(populate_by_name=True, from_attributes=True)


class LogQueryFilter(BaseModel):
    """Validated query parameters for searching and filtering application logs."""

    level: Optional[str] = Field(default=None, description="Filter by log level (INFO, WARNING, ERROR, CRITICAL)")
    service: Optional[str] = Field(default=None, description="Filter by emitting microservice")
    environment: Optional[str] = Field(default=None, description="Filter by environment (production, staging, etc.)")
    request_id: Optional[str] = Field(default=None, description="Exact lookup by request correlation ID")
    trace_id: Optional[str] = Field(default=None, description="Exact lookup by distributed trace ID")
    tenant_id: Optional[str] = Field(default=None, description="Tenant boundary isolation filter")
    search: Optional[str] = Field(default=None, max_length=200, description="Bounded full-text query on message")
    start_time: Optional[datetime] = Field(default=None, description="ISO-8601 start timestamp")
    end_time: Optional[datetime] = Field(default=None, description="ISO-8601 end timestamp")
    limit: int = Field(default=50, ge=1, le=500, description="Max results per page")
    offset: int = Field(default=0, ge=0, description="Offset for pagination")

    @field_validator("level")
    @classmethod
    def normalize_level(cls, v: Optional[str]) -> Optional[str]:
        if v:
            return v.strip().upper()
        return None


class LogTimelineBucket(BaseModel):
    """Time-series aggregation bucket for log volumes."""

    timestamp: str
    count: int


class LogAggregationResponse(BaseModel):
    """Aggregated metrics across application logs for visualization and diagnostics."""

    total_logs: int
    by_level: Dict[str, int]
    by_service: Dict[str, int]
    by_route: Dict[str, int]
    by_status_code: Dict[str, int]
    timeline: List[LogTimelineBucket]

    model_config = ConfigDict(from_attributes=True)


class LogServicesResponse(BaseModel):
    """List of all distinct services currently emitting logs."""

    services: List[str]


__all__ = [
    "LogExceptionDetail",
    "LogEntryRead",
    "LogQueryFilter",
    "LogTimelineBucket",
    "LogAggregationResponse",
    "LogServicesResponse",
]
