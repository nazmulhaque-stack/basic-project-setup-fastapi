"""The `provide()` dynamic dependency factory: Session -> Cache -> Crypto -> Elasticsearch -> Service.

Split out of a single `deps.py` so the *mechanism* (this file) and the
*per-entity configuration that uses it* (`services.py`) can change
independently — adding entity #5 never requires touching this file.
"""

from collections.abc import Callable
import inspect
from typing import Annotated, Any, Optional, TypeVar, Union

from fastapi import Depends, Request
from redis.asyncio import Redis, RedisCluster

from app.core.config import Settings, get_settings
from app.core.elasticsearch import IElasticsearchService, get_es
from app.core.security import CryptoService
from app.db.pool import UnitOfWork, get_session
from app.services.base import BaseService
from app.services.cache import DynamicRedisCache
from app.services.interface import ICacheService, NullCache

ServiceT = TypeVar("ServiceT", bound=BaseService)  # type: ignore[type-arg]

# Standard session dependency
SessionDep = Annotated[UnitOfWork, Depends(get_session)]


def get_crypto(request: Request) -> Optional[CryptoService]:
    """Extracts initialized CryptoService instance from application state."""
    return getattr(request.app.state, "crypto_service", None)


def get_redis_client(request: Request) -> Optional[Union[RedisCluster, Redis]]:
    """Extracts active Redis cluster / standalone client from application state."""
    return getattr(request.app.state, "redis", None)


def provide(service_cls: type[ServiceT], cache_prefix: Optional[str] = None) -> Callable[..., ServiceT]:
    """Dynamic dependency factory injecting session, Redis cache, crypto, and Elasticsearch into domain services."""

    prefix = cache_prefix or service_cls.__name__.lower().replace("service", "")

    def _provider(
        request: Request,
        session: SessionDep,
        es: Annotated[IElasticsearchService, Depends(get_es)],
        settings: Annotated[Settings, Depends(get_settings)],
    ) -> ServiceT:
        redis_client = getattr(request.app.state, "redis", None)
        cache: ICacheService
        if redis_client is not None:
            cache = DynamicRedisCache(
                redis=redis_client,
                prefix=prefix,
                item_ttl=settings.CACHE_ITEM_TTL,
                list_ttl=settings.CACHE_LIST_TTL,
                negative_ttl=settings.CACHE_NEGATIVE_TTL,
                lock_ttl_ms=settings.CACHE_LOCK_TTL_MS,
                flags=settings.get_cache_flags(),
            )
        else:
            cache = NullCache()

        crypto = getattr(request.app.state, "crypto_service", None)

        init_sig = inspect.signature(service_cls.__init__)
        kwargs: dict[str, Any] = {}

        if "db" in init_sig.parameters:
            kwargs["db"] = session
        elif "session" in init_sig.parameters:
            kwargs["session"] = session

        if "cache" in init_sig.parameters:
            kwargs["cache"] = cache

        if "crypto" in init_sig.parameters:
            kwargs["crypto"] = crypto

        if "elasticsearch" in init_sig.parameters:
            kwargs["elasticsearch"] = es

        if "outbox_enabled" in init_sig.parameters:
            kwargs["outbox_enabled"] = settings.KAFKA_ENABLED

        if "auto_outbox_events" in init_sig.parameters:
            kwargs["auto_outbox_events"] = False

        if "outbox_topic" in init_sig.parameters:
            kwargs["outbox_topic"] = settings.KAFKA_EVENTS_TOPIC

        return service_cls(**kwargs)

    _provider.__name__ = f"get_{service_cls.__name__.lower()}"
    return _provider


__all__ = [
    "provide",
    "SessionDep",
    "get_crypto",
    "get_redis_client",
]
