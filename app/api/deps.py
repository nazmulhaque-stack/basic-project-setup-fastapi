"""Application Infrastructure Dependency Injection (FastAPI Official Documentation Style).

Centralizes and standardizes core infrastructure dependencies:
- PostgreSQL asyncpg UnitOfWork session (Database)
- Redis cluster / standalone client (Caching & Rate Limiting)
- Elasticsearch client (CQRS Search Engine)
- AES-256-GCM AEAD cryptographic security & HMAC blind indexing (Crypto)
"""

from collections.abc import AsyncGenerator
from typing import Annotated, Any, Optional, Union

from fastapi import Depends, Request
from redis.asyncio import Redis, RedisCluster

from app.core.cache_interface import NullCache
from app.core.config import Settings, get_settings
from app.core.elasticsearch import IElasticsearchService, NullElasticsearch
from app.core.security import CryptoService
from app.db.pool import UnitOfWork, get_db as _pool_get_db
from app.services.executor import DynamicServiceExecutor


# ------------------------------------------------------------------------------
# 1. Database Session Dependency (asyncpg UnitOfWork)
# ------------------------------------------------------------------------------
async def get_db(request: Request) -> AsyncGenerator[UnitOfWork, None]:
    """Provides a transactional UnitOfWork on a pooled asyncpg connection."""
    async for uow in _pool_get_db(request):
        yield uow


SessionDep = Annotated[UnitOfWork, Depends(get_db)]
DbDep = Annotated[UnitOfWork, Depends(get_db)]


# ------------------------------------------------------------------------------
# 2. Redis Client Dependency
# ------------------------------------------------------------------------------
def get_redis(request: Request) -> Optional[Union[RedisCluster, Redis]]:
    """Dependency injecting the active Redis client from application state."""
    return getattr(request.app.state, "redis", None)


RedisDep = Annotated[Optional[Union[RedisCluster, Redis]], Depends(get_redis)]


# ------------------------------------------------------------------------------
# 3. Elasticsearch Dependency (CQRS Search Engine)
# ------------------------------------------------------------------------------
def get_elasticsearch(request: Request) -> IElasticsearchService:
    """Dependency injecting Elasticsearch client from application state."""
    es = getattr(request.app.state, "elasticsearch", None)
    if es is not None:
        return es
    return NullElasticsearch()


ElasticsearchDep = Annotated[IElasticsearchService, Depends(get_elasticsearch)]


# ------------------------------------------------------------------------------
# 4. AES-256-GCM AEAD Cryptographic Security Dependency
# ------------------------------------------------------------------------------
def get_crypto(
    request: Request,
    settings: Annotated[Settings, Depends(get_settings)],
) -> CryptoService:
    """Dependency injecting AES-256-GCM AEAD CryptoService & Blind Indexer."""
    crypto = getattr(request.app.state, "crypto_service", None)
    if crypto is not None:
        return crypto
    return CryptoService(
        keys=settings.get_encryption_keys(),
        active_version=settings.ACTIVE_KEY_VERSION,
        blind_index_key=settings.get_blind_index_key(),
    )


CryptoDep = Annotated[CryptoService, Depends(get_crypto)]


# ------------------------------------------------------------------------------
# 5. Dynamic Service Executor Dependency (Type-Safe Query/Redis/ES Orchestrator)
# ------------------------------------------------------------------------------
def get_executor(
    session: SessionDep,
    redis: RedisDep,
    es: ElasticsearchDep,
    request: Request,
    settings: Annotated[Settings, Depends(get_settings)],
) -> DynamicServiceExecutor:
    """Dependency constructing a DynamicServiceExecutor with active DB, Redis, and ES."""
    cache_service = NullCache()
    if redis is not None:
        from app.services.cache import DynamicRedisCache

        cache_service = DynamicRedisCache(
            redis=redis,
            prefix="app",
            item_ttl=settings.CACHE_ITEM_TTL,
            list_ttl=settings.CACHE_LIST_TTL,
            negative_ttl=settings.CACHE_NEGATIVE_TTL,
            lock_ttl_ms=settings.CACHE_LOCK_TTL_MS,
            flags=settings.get_cache_flags(),
        )

    worker = None
    if hasattr(request, "app") and hasattr(request.app, "state"):
        worker = getattr(request.app.state, "outbox_worker", None)

    return DynamicServiceExecutor(
        db=session,
        cache=cache_service,
        elasticsearch=es,
        outbox_worker=worker,
    )


ExecutorDep = Annotated[DynamicServiceExecutor, Depends(get_executor)]


__all__ = [
    # Database
    "get_db",
    "SessionDep",
    "DbDep",
    # Redis
    "get_redis",
    "RedisDep",
    # Elasticsearch
    "get_elasticsearch",
    "ElasticsearchDep",
    # Crypto (AES-256-GCM AEAD)
    "get_crypto",
    "CryptoDep",
    # Dynamic Executor
    "get_executor",
    "ExecutorDep",
]
