"""Dependency providers that don't fit the `provide()`/BaseService shape:
log search (reads Elasticsearch directly, no repository) and the Redis
temporary-state helper (locks/idempotency/challenges, no entity at all).
"""

from typing import Annotated, Optional

from fastapi import Depends, Request

from app.core.config import Settings, get_settings
from app.core.elasticsearch import IElasticsearchService, get_es


def get_log_search_service(
    es: Annotated[IElasticsearchService, Depends(get_es)],
    settings: Annotated[Settings, Depends(get_settings)],
) -> "LogSearchService":
    from app.services.log_search import LogSearchService
    return LogSearchService(es=es, settings=settings)


LogSearchServiceDep = Annotated["LogSearchService", Depends(get_log_search_service)]


def get_temp_state(request: Request) -> Optional["RedisTemporaryState"]:
    from app.core.redis import RedisTemporaryState
    redis_client = getattr(request.app.state, "redis", None)
    if redis_client is not None:
        return RedisTemporaryState(redis_client)
    return None


TempStateDep = Annotated[Optional["RedisTemporaryState"], Depends(get_temp_state)]


__all__ = [
    "get_log_search_service",
    "LogSearchServiceDep",
    "get_temp_state",
    "TempStateDep",
]
