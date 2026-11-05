"""Elasticsearch 8 Asynchronous CQRS Search Engine Client.

Provides high-throughput search offloading for full-text and fuzzy queries.
Includes Null Object Pattern (NullElasticsearch) to fail-open gracefully when ES is offline.
"""

from abc import ABC, abstractmethod
import logging
from typing import Any, Dict, List, Optional, Tuple, Union

from fastapi import Request

try:
    from elasticsearch import AsyncElasticsearch, NotFoundError as ESNotFoundError
except ImportError:
    AsyncElasticsearch = None  # type: ignore[misc,assignment]
    ESNotFoundError = Exception  # type: ignore[misc,assignment]

from app.core.config import Settings, get_settings

logger = logging.getLogger(__name__)


class IElasticsearchService(ABC):
    """Abstract interface for Elasticsearch search operations."""

    @abstractmethod
    async def index_document(self, index: str, id_: str, document: Dict[str, Any]) -> bool:
        """Indexes or updates a document in Elasticsearch."""
        pass

    @abstractmethod
    async def update_document(self, index: str, id_: str, document: Dict[str, Any]) -> bool:
        """Partially updates a document in Elasticsearch."""
        pass

    @abstractmethod
    async def delete_document(self, index: str, id_: str) -> bool:
        """Deletes a document from Elasticsearch."""
        pass

    @abstractmethod
    async def search(
        self,
        index: str,
        query: Union[str, Dict[str, Any]],
        limit: int = 20,
        offset: int = 0,
    ) -> Tuple[List[Dict[str, Any]], int]:
        """Executes full-text or structured fuzzy search against Elasticsearch index."""
        pass

    @abstractmethod
    async def search_raw(
        self,
        index: str,
        body: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Executes raw search/aggregation query returning Elasticsearch response."""
        pass

    @abstractmethod
    async def ping(self) -> bool:
        """Checks cluster connectivity."""
        pass

    @abstractmethod
    async def close(self) -> None:
        """Closes underlying client connections."""
        pass


class NullElasticsearch(IElasticsearchService):
    """Safe no-op implementation when Elasticsearch is disabled or offline."""

    async def index_document(self, index: str, id_: str, document: Dict[str, Any]) -> bool:
        return True

    async def update_document(self, index: str, id_: str, document: Dict[str, Any]) -> bool:
        return True

    async def delete_document(self, index: str, id_: str) -> bool:
        return True

    async def search(
        self,
        index: str,
        query: Union[str, Dict[str, Any]],
        limit: int = 20,
        offset: int = 0,
    ) -> Tuple[List[Dict[str, Any]], int]:
        return [], 0

    async def search_raw(
        self,
        index: str,
        body: Dict[str, Any],
    ) -> Dict[str, Any]:
        return {"hits": {"total": {"value": 0}, "hits": []}, "aggregations": {}}

    async def ping(self) -> bool:
        return True

    async def close(self) -> None:
        pass


class AsyncElasticsearchService(IElasticsearchService):
    """Production asynchronous Elasticsearch 8 client."""

    def __init__(self, client: Any, index_prefix: str = "demo") -> None:
        self.client: Any = client
        self.prefix: str = index_prefix

    def _full_index_name(self, index: str) -> str:
        if index.startswith(f"{self.prefix}_") or index == self.prefix:
            return index
        return f"{self.prefix}_{index}"

    async def index_document(self, index: str, id_: str, document: Dict[str, Any]) -> bool:
        full_index = self._full_index_name(index)
        try:
            await self.client.index(index=full_index, id=str(id_), document=document)
            return True
        except Exception as exc:
            logger.warning("Elasticsearch index error on %s/%s: %s", full_index, id_, exc)
            return False

    async def update_document(self, index: str, id_: str, document: Dict[str, Any]) -> bool:
        full_index = self._full_index_name(index)
        try:
            await self.client.update(index=full_index, id=str(id_), doc=document, doc_as_upsert=True)
            return True
        except Exception as exc:
            logger.warning("Elasticsearch update error on %s/%s: %s", full_index, id_, exc)
            return False

    async def delete_document(self, index: str, id_: str) -> bool:
        full_index = self._full_index_name(index)
        try:
            await self.client.delete(index=full_index, id=str(id_))
            return True
        except ESNotFoundError:
            return True
        except Exception as exc:
            logger.warning("Elasticsearch delete error on %s/%s: %s", full_index, id_, exc)
            return False

    async def search(
        self,
        index: str,
        query: Union[str, Dict[str, Any]],
        limit: int = 20,
        offset: int = 0,
    ) -> Tuple[List[Dict[str, Any]], int]:
        full_index = self._full_index_name(index)
        if isinstance(query, str):
            es_query: Dict[str, Any] = {
                "multi_match": {
                    "query": query,
                    "fields": ["*"],
                    "fuzziness": "AUTO",
                }
            }
        else:
            es_query = query

        try:
            resp = await self.client.search(
                index=full_index,
                query=es_query,
                from_=offset,
                size=limit,
            )
            total = int(resp["hits"]["total"]["value"])
            hits = [hit["_source"] for hit in resp["hits"]["hits"]]
            return hits, total
        except Exception as exc:
            logger.warning("Elasticsearch search failed on %s: %s", full_index, exc)
            return [], 0

    async def search_raw(
        self,
        index: str,
        body: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Executes query with body directly against Elasticsearch."""
        full_index = self._full_index_name(index)
        try:
            return await self.client.search(index=full_index, **body)
        except Exception as exc:
            logger.warning("Elasticsearch search_raw failed on %s: %s", full_index, exc)
            return {"hits": {"total": {"value": 0}, "hits": []}, "aggregations": {}}

    async def ping(self) -> bool:
        try:
            return bool(await self.client.ping())
        except Exception:
            return False

    async def close(self) -> None:
        try:
            if hasattr(self.client, "close"):
                await self.client.close()
        except Exception as exc:
            logger.warning("Error closing Elasticsearch client: %s", exc)


def create_elasticsearch_client(settings: Optional[Settings] = None) -> Optional[Any]:
    """Creates a configured AsyncElasticsearch official client or None if disabled."""
    cfg = settings or get_settings()
    if not cfg.ELASTICSEARCH_ENABLED or AsyncElasticsearch is None:
        return None

    client_kwargs: Dict[str, Any] = {
        "hosts": [cfg.ELASTICSEARCH_URL],
        "request_timeout": cfg.ELASTICSEARCH_TIMEOUT,
        "max_retries": cfg.ELASTICSEARCH_MAX_RETRIES,
        "verify_certs": cfg.ELASTICSEARCH_VERIFY_CERTS,
    }

    if cfg.ELASTICSEARCH_CA_CERTS:
        client_kwargs["ca_certs"] = cfg.ELASTICSEARCH_CA_CERTS

    if cfg.ELASTICSEARCH_API_KEY:
        client_kwargs["api_key"] = cfg.ELASTICSEARCH_API_KEY
    elif cfg.ELASTICSEARCH_USERNAME and cfg.ELASTICSEARCH_PASSWORD:
        client_kwargs["basic_auth"] = (cfg.ELASTICSEARCH_USERNAME, cfg.ELASTICSEARCH_PASSWORD)

    try:
        return AsyncElasticsearch(**client_kwargs)
    except Exception as exc:
        logger.warning("Could not instantiate AsyncElasticsearch client: %s", exc)
        return None


def get_es(request: Request) -> IElasticsearchService:
    """Dependency injecting Elasticsearch service from application state."""
    es = getattr(request.app.state, "elasticsearch", None)
    if es is not None:
        return es
    return NullElasticsearch()


__all__ = [
    "IElasticsearchService",
    "NullElasticsearch",
    "AsyncElasticsearchService",
    "create_elasticsearch_client",
    "get_es",
]
