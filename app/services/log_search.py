"""Domain service for centralized application-log search, filtering, and aggregations.

Enforces tenant isolation, bounds query complexity and time ranges, and communicates
with Elasticsearch using high-performance search and aggregation DSL.
"""

from datetime import datetime, timedelta, timezone
import logging
from typing import Any, Dict, List, Optional, Tuple

from app.core.config import Settings, get_settings
from app.core.elasticsearch import IElasticsearchService
from app.schemas.log import (
    LogAggregationResponse,
    LogEntryRead,
    LogQueryFilter,
    LogServicesResponse,
    LogTimelineBucket,
)

logger = logging.getLogger(__name__)

MAX_TIME_RANGE_DAYS = 30
DEFAULT_QUERY_TIMEOUT = "5s"


class LogSearchService:
    """Service orchestrating validated log searches and aggregations against Elasticsearch."""

    def __init__(
        self,
        es: IElasticsearchService,
        log_index: Optional[str] = None,
        settings: Optional[Settings] = None,
    ) -> None:
        self.es: IElasticsearchService = es
        cfg = settings or get_settings()
        self.log_index: str = log_index or cfg.ELASTICSEARCH_LOG_INDEX

    def _build_bool_query(self, filters: LogQueryFilter) -> Dict[str, Any]:
        """Constructs safe, validated Elasticsearch query DSL with bounded filters."""
        must: List[Dict[str, Any]] = []
        filter_clauses: List[Dict[str, Any]] = []

        # 1. Full-text search on message with bounded query
        if filters.search:
            must.append({
                "match": {
                    "message": {
                        "query": filters.search,
                        "operator": "and",
                        "fuzziness": "AUTO",
                    }
                }
            })
        else:
            must.append({"match_all": {}})

        # 2. Exact keyword filters
        if filters.level:
            filter_clauses.append({"term": {"level": filters.level}})
        if filters.service:
            filter_clauses.append({"term": {"service": filters.service}})
        if filters.environment:
            filter_clauses.append({"term": {"environment": filters.environment}})
        if filters.request_id:
            filter_clauses.append({"term": {"request_id": filters.request_id}})
        if filters.trace_id:
            filter_clauses.append({"term": {"trace_id": filters.trace_id}})
        if filters.tenant_id:
            filter_clauses.append({"term": {"tenant_id": filters.tenant_id}})

        # 3. Time range filtering with safety bounds (max 30 days)
        now = datetime.now(timezone.utc)
        start = filters.start_time or (now - timedelta(days=7))
        end = filters.end_time or now

        # Bound maximum range
        if (end - start).days > MAX_TIME_RANGE_DAYS:
            start = end - timedelta(days=MAX_TIME_RANGE_DAYS)

        range_clause: Dict[str, Any] = {
            "range": {
                "@timestamp": {
                    "gte": start.isoformat(),
                    "lte": end.isoformat(),
                }
            }
        }
        filter_clauses.append(range_clause)

        return {
            "bool": {
                "must": must,
                "filter": filter_clauses,
            }
        }

    async def search_logs(self, filters: LogQueryFilter) -> Tuple[List[LogEntryRead], int]:
        """Executes paginated log search with deterministic sorting."""
        query = self._build_bool_query(filters)
        body: Dict[str, Any] = {
            "query": query,
            "sort": [
                {"@timestamp": {"order": "desc"}},
                {"_doc": {"order": "asc"}},
            ],
            "from_": filters.offset,
            "size": min(filters.limit, 500),
            "timeout": DEFAULT_QUERY_TIMEOUT,
        }

        resp = await self.es.search_raw(self.log_index, body)
        hits_data = resp.get("hits", {})
        total = int(hits_data.get("total", {}).get("value", 0))

        records: List[LogEntryRead] = []
        for hit in hits_data.get("hits", []):
            src = hit.get("_source", {})
            try:
                records.append(LogEntryRead(**src))
            except Exception as exc:
                logger.debug("Failed parsing log document from ES: %s", exc)

        return records, total

    async def get_errors(self, filters: LogQueryFilter) -> Tuple[List[LogEntryRead], int]:
        """Convenience method returning logs with level ERROR or CRITICAL."""
        query = self._build_bool_query(filters)
        # Ensure terms filter on level matches errors
        query["bool"]["filter"].append({
            "terms": {"level": ["ERROR", "CRITICAL"]}
        })

        body: Dict[str, Any] = {
            "query": query,
            "sort": [{"@timestamp": {"order": "desc"}}],
            "from_": filters.offset,
            "size": min(filters.limit, 500),
            "timeout": DEFAULT_QUERY_TIMEOUT,
        }

        resp = await self.es.search_raw(self.log_index, body)
        hits_data = resp.get("hits", {})
        total = int(hits_data.get("total", {}).get("value", 0))

        records: List[LogEntryRead] = []
        for hit in hits_data.get("hits", []):
            records.append(LogEntryRead(**hit.get("_source", {})))

        return records, total

    async def aggregate_logs(
        self,
        filters: LogQueryFilter,
        interval: str = "1h",
    ) -> LogAggregationResponse:
        """Executes multi-facet aggregations by level, service, route, status code, and date histogram."""
        query = self._build_bool_query(filters)

        body: Dict[str, Any] = {
            "query": query,
            "size": 0,  # Do not return documents, only aggregations
            "timeout": DEFAULT_QUERY_TIMEOUT,
            "aggs": {
                "by_level": {"terms": {"field": "level", "size": 10}},
                "by_service": {"terms": {"field": "service", "size": 20}},
                "by_route": {"terms": {"field": "route", "size": 30}},
                "by_status_code": {"terms": {"field": "status_code", "size": 20}},
                "timeline": {
                    "date_histogram": {
                        "field": "@timestamp",
                        "fixed_interval": interval,
                        "min_doc_count": 0,
                    }
                },
            },
        }

        resp = await self.es.search_raw(self.log_index, body)
        total = int(resp.get("hits", {}).get("total", {}).get("value", 0))
        aggs = resp.get("aggregations", {})

        by_level = {b["key"]: int(b["doc_count"]) for b in aggs.get("by_level", {}).get("buckets", [])}
        by_service = {b["key"]: int(b["doc_count"]) for b in aggs.get("by_service", {}).get("buckets", [])}
        by_route = {b["key"]: int(b["doc_count"]) for b in aggs.get("by_route", {}).get("buckets", [])}
        by_status_code = {str(b["key"]): int(b["doc_count"]) for b in aggs.get("by_status_code", {}).get("buckets", [])}

        timeline = [
            LogTimelineBucket(
                timestamp=str(b.get("key_as_string") or b.get("key", "")),
                count=int(b["doc_count"]),
            )
            for b in aggs.get("timeline", {}).get("buckets", [])
        ]

        return LogAggregationResponse(
            total_logs=total,
            by_level=by_level,
            by_service=by_service,
            by_route=by_route,
            by_status_code=by_status_code,
            timeline=timeline,
        )

    async def get_services(self) -> LogServicesResponse:
        """Returns distinct services from keyword aggregation."""
        body: Dict[str, Any] = {
            "size": 0,
            "aggs": {
                "unique_services": {
                    "terms": {"field": "service", "size": 100}
                }
            },
        }
        resp = await self.es.search_raw(self.log_index, body)
        buckets = resp.get("aggregations", {}).get("unique_services", {}).get("buckets", [])
        services = [b["key"] for b in buckets]
        return LogServicesResponse(services=services)


__all__ = ["LogSearchService", "MAX_TIME_RANGE_DAYS"]
