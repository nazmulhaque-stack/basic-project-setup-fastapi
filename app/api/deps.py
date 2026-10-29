"""FastAPI dependency providers: Session -> Cache -> Crypto -> Elasticsearch -> Service.

Clean Architecture boundary: Endpoints and Controllers only ever see domain Services.
"""

from collections.abc import Callable
import inspect
from typing import Annotated, Any, Optional, TypeVar, Union

from fastapi import Depends, Request
from redis.asyncio import Redis, RedisCluster
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import Settings, get_settings
from app.core.elasticsearch import IElasticsearchService, get_es
from app.core.security import CryptoService
from app.db.session import get_session
from app.services.base import BaseService, DynamicRedisCache
from app.services.interface import ICacheService, NullCache
from app.services.category import CategoryService
from app.services.product import ProductService
from app.services.user import UserService

ServiceT = TypeVar("ServiceT", bound=BaseService)  # type: ignore[type-arg]

# Standard session dependency
SessionDep = Annotated[AsyncSession, Depends(get_session)]


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

        return service_cls(**kwargs)

    _provider.__name__ = f"get_{service_cls.__name__.lower()}"
    return _provider


# Pre-configured domain service dependency providers
get_user_service = provide(UserService, cache_prefix="users")
get_product_service = provide(ProductService, cache_prefix="products")
get_category_service = provide(CategoryService, cache_prefix="categories")

UserServiceDep = Annotated[UserService, Depends(get_user_service)]
ProductServiceDep = Annotated[ProductService, Depends(get_product_service)]
CategoryServiceDep = Annotated[CategoryService, Depends(get_category_service)]


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
    "provide",
    "SessionDep",
    "get_crypto",
    "get_redis_client",
    "get_user_service",
    "get_product_service",
    "get_category_service",
    "get_log_search_service",
    "get_temp_state",
    "UserServiceDep",
    "ProductServiceDep",
    "CategoryServiceDep",
    "LogSearchServiceDep",
    "TempStateDep",
]
