"""Elasticsearch index lifecycle manager.

Handles index creation with mappings, zero-downtime reindexing via aliases,
index template management, and health monitoring.

Index naming convention: <prefix>_<entity>_v<N>  (e.g. app_products_v1)
Aliases:  <prefix>_<entity>_read  -> current read index
          <prefix>_<entity>_write -> current write index
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Union

from fastapi import Request

from app.core.config import get_settings
from app.core.elasticsearch_registry import ELASTICSEARCH_INDEXES

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Graceful degradation when elasticsearch is not installed
# ---------------------------------------------------------------------------

try:
    from elasticsearch import AsyncElasticsearch
    from elasticsearch import NotFoundError as ESNotFoundError
    from elasticsearch import RequestError as ESRequestError

    _ES_AVAILABLE = True
except ImportError:  # pragma: no cover
    AsyncElasticsearch = None  # type: ignore[assignment,misc]
    ESNotFoundError = Exception  # type: ignore[assignment,misc]
    ESRequestError = Exception  # type: ignore[assignment,misc]
    _ES_AVAILABLE = False
    logger.warning(
        "elasticsearch-py is not installed — ElasticsearchIndexManager operates "
        "in no-op (NullIndexManager) mode. "
        "Install it with: pip install elasticsearch[async]"
    )

# ---------------------------------------------------------------------------
# Default mappings per entity type
# ---------------------------------------------------------------------------

_DEFAULT_INDEX_SETTINGS: Dict[str, Any] = {
    "number_of_shards": 1,
    "number_of_replicas": 1,
    "refresh_interval": "1s",
}

_ENTITY_MAPPINGS: Dict[str, Dict[str, Any]] = {
    "products": {
        "properties": {
            "name": {"type": "text", "analyzer": "standard"},
            "sku": {"type": "keyword"},
            "price_cents": {"type": "long"},
            "category": {"type": "keyword"},
            "is_active": {"type": "boolean"},
            "stock": {"type": "integer"},
            "created_at": {"type": "date", "format": "strict_date_optional_time||epoch_millis"},
        }
    },
    "users": {
        "properties": {
            "email": {"type": "keyword"},
            "name": {"type": "text", "analyzer": "standard"},
            "is_active": {"type": "boolean"},
            "created_at": {"type": "date", "format": "strict_date_optional_time||epoch_millis"},
        }
    },
    "categories": {
        "properties": {
            "name": {"type": "text", "analyzer": "standard"},
            "is_active": {"type": "boolean"},
            "created_at": {"type": "date", "format": "strict_date_optional_time||epoch_millis"},
        }
    },
    "employees": {
        "properties": {
            "name": {"type": "text", "analyzer": "standard"},
            "email": {"type": "keyword"},
            "is_active": {"type": "boolean"},
            "created_at": {"type": "date", "format": "strict_date_optional_time||epoch_millis"},
        }
    },
}


# ---------------------------------------------------------------------------
# ElasticsearchIndexManager
# ---------------------------------------------------------------------------


class ElasticsearchIndexManager:
    """Manages the full lifecycle of Elasticsearch indices.

    Responsibilities:
    - Idempotent index creation with custom mappings and settings.
    - Dual-alias pattern (``*_read`` / ``*_write``) for zero-downtime
      reindexing.
    - Atomic alias swap during reindexing so read traffic is never dropped.
    - Health monitoring helpers used by liveness/readiness probes.

    All async methods catch and log exceptions; they never propagate to
    callers, returning ``False`` / empty dicts on failure so primary
    application flow is unaffected.

    Args:
        client:       An active :class:`~elasticsearch.AsyncElasticsearch`
                      instance, or ``None`` when ES is disabled.
        index_prefix: Namespace prefix applied to all index and alias names
                      (default ``"app"``).
    """

    def __init__(
        self,
        client: Optional[Any],
        index_prefix: str = "app",
    ) -> None:
        self._client: Optional[Any] = client
        self._prefix: str = index_prefix.rstrip("_")

    # ------------------------------------------------------------------
    # Internal naming helpers
    # ------------------------------------------------------------------

    def _index_name(self, entity: str, version: int = 1) -> str:
        """Build the versioned physical index name.

        Args:
            entity:  Logical entity name (e.g. ``"products"``).
            version: Version counter (default ``1``).

        Returns:
            E.g. ``"app_products_v1"``.
        """
        return f"{self._prefix}_{entity}_v{version}"

    def _alias_name(self, entity: str, alias_type: str = "read") -> str:
        """Build an alias name for an entity.

        Args:
            entity:     Logical entity name.
            alias_type: ``"read"`` or ``"write"`` (default ``"read"``).

        Returns:
            E.g. ``"app_products_read"``.
        """
        return f"{self._prefix}_{entity}_{alias_type}"

    # ------------------------------------------------------------------
    # Index creation
    # ------------------------------------------------------------------

    async def create_index(
        self,
        entity: str,
        mappings: Dict[str, Any],
        settings: Optional[Dict[str, Any]] = None,
        version: int = 1,
    ) -> bool:
        """Create a versioned index with mappings, settings, and aliases.

        The operation is idempotent: if the index already exists the method
        returns ``True`` without modifying the existing index or aliases.

        Args:
            entity:   Logical entity name (e.g. ``"products"``).
            mappings: Elasticsearch mappings body dict (``{"properties": {...}}``).
            settings: Optional index settings dict; defaults to
                      :data:`_DEFAULT_INDEX_SETTINGS`.
            version:  Index version number (default ``1``).

        Returns:
            ``True`` on success or if the index already existed, ``False``
            on Elasticsearch or network failure.
        """
        if self._client is None:
            logger.debug("create_index skipped — no Elasticsearch client.")
            return False

        index_name = self._index_name(entity, version)
        read_alias = self._alias_name(entity, "read")
        write_alias = self._alias_name(entity, "write")

        try:
            exists: bool = await self._client.indices.exists(index=index_name)
            if exists:
                logger.debug("Index %s already exists — skipping creation.", index_name)
                return True

            body: Dict[str, Any] = {
                "mappings": mappings,
                "settings": settings or _DEFAULT_INDEX_SETTINGS,
                "aliases": {
                    read_alias: {},
                    write_alias: {},
                },
            }
            await self._client.indices.create(index=index_name, body=body)
            logger.info(
                "Created index %s with aliases %s and %s.",
                index_name,
                read_alias,
                write_alias,
            )
            return True
        except Exception as exc:
            logger.warning("create_index failed for entity %s v%d: %s", entity, version, exc)
            return False

    async def ensure_index(
        self,
        entity: str,
        mappings: Dict[str, Any],
    ) -> bool:
        """Ensure index v1 exists, creating it if necessary.

        A thin convenience wrapper around :meth:`create_index` that always
        targets version 1 and swallows all exceptions.

        Args:
            entity:   Logical entity name.
            mappings: Mappings body dict.

        Returns:
            ``True`` if the index exists or was successfully created,
            ``False`` otherwise.
        """
        try:
            return await self.create_index(entity, mappings, version=1)
        except Exception as exc:
            logger.warning("ensure_index failed for entity %s: %s", entity, exc)
            return False

    # ------------------------------------------------------------------
    # Zero-downtime reindexing
    # ------------------------------------------------------------------

    async def reindex(
        self,
        entity: str,
        new_mappings: Dict[str, Any],
        new_version: int,
    ) -> bool:
        """Perform a zero-downtime schema migration via alias swap.

        Steps:
        1. Determine the current write index from the ``*_write`` alias.
        2. Create the new versioned index (with ``new_mappings``).
        3. POST ``/_reindex`` to copy all documents from old → new index.
        4. Atomically swap the read *and* write aliases from old to new.
        5. Log completion; old index is retained for manual rollback if needed.

        The read alias points to the old index until the atomic alias swap
        completes so that search queries are never routed to an incomplete
        index.

        Args:
            entity:       Logical entity name.
            new_mappings: Mappings for the new index version.
            new_version:  Version number for the new index (must be > current).

        Returns:
            ``True`` on success, ``False`` on any failure.
        """
        if self._client is None:
            logger.debug("reindex skipped — no Elasticsearch client.")
            return False

        write_alias = self._alias_name(entity, "write")
        read_alias = self._alias_name(entity, "read")
        new_index = self._index_name(entity, new_version)

        try:
            # 1. Discover current index behind the write alias
            old_index: Optional[str] = None
            try:
                alias_info: Dict[str, Any] = await self._client.indices.get_alias(name=write_alias)
                # alias_info format: {<index_name>: {"aliases": {...}}}
                old_index = next(iter(alias_info.keys()), None)
            except (ESNotFoundError, Exception) as exc:
                logger.warning(
                    "reindex: could not resolve current write alias %s: %s — "
                    "will create new index without reindexing existing data.",
                    write_alias,
                    exc,
                )

            # 2. Create the new index (without aliases to prevent premature traffic)
            body: Dict[str, Any] = {
                "mappings": new_mappings,
                "settings": _DEFAULT_INDEX_SETTINGS,
            }
            new_exists = await self._client.indices.exists(index=new_index)
            if not new_exists:
                await self._client.indices.create(index=new_index, body=body)
                logger.info("reindex: created new index %s", new_index)
            else:
                logger.info("reindex: new index %s already exists — skipping creation.", new_index)

            # 3. Reindex documents from old to new (if old index was found)
            if old_index and old_index != new_index:
                reindex_body: Dict[str, Any] = {
                    "source": {"index": old_index},
                    "dest": {"index": new_index},
                }
                result = await self._client.reindex(body=reindex_body, wait_for_completion=True)
                took = result.get("took", "?")
                total = result.get("total", "?")
                logger.info(
                    "reindex: copied %s documents from %s to %s in %s ms.",
                    total,
                    old_index,
                    new_index,
                    took,
                )

            # 4. Atomic alias swap: add new aliases, remove old aliases
            actions: List[Dict[str, Any]] = [
                {"add": {"index": new_index, "alias": read_alias}},
                {"add": {"index": new_index, "alias": write_alias}},
            ]
            if old_index and old_index != new_index:
                actions += [
                    {"remove": {"index": old_index, "alias": read_alias}},
                    {"remove": {"index": old_index, "alias": write_alias}},
                ]
            await self._client.indices.update_aliases(body={"actions": actions})
            logger.info(
                "reindex: aliases %s and %s now point to %s.",
                read_alias,
                write_alias,
                new_index,
            )
            return True

        except Exception as exc:
            logger.warning("reindex failed for entity %s v%d: %s", entity, new_version, exc)
            return False

    # ------------------------------------------------------------------
    # Index deletion
    # ------------------------------------------------------------------

    async def delete_index(self, entity: str, version: int) -> bool:
        """Delete a specific versioned index.

        .. warning::
            This operation is irreversible.  Ensure the index is no longer
            referenced by any alias before calling this method.

        Args:
            entity:  Logical entity name.
            version: Version number of the index to delete.

        Returns:
            ``True`` on success, ``False`` on failure.
        """
        if self._client is None:
            return False

        index_name = self._index_name(entity, version)
        try:
            await self._client.indices.delete(index=index_name)
            logger.info("Deleted index %s.", index_name)
            return True
        except ESNotFoundError:
            logger.debug("delete_index: index %s not found — treating as success.", index_name)
            return True
        except Exception as exc:
            logger.warning("delete_index failed for %s: %s", index_name, exc)
            return False

    # ------------------------------------------------------------------
    # Statistics & health
    # ------------------------------------------------------------------

    async def get_index_stats(self, entity: str) -> Dict[str, Any]:
        """Retrieve statistics for all versioned indices of *entity*.

        Queries the ``_cat/indices`` API with the wildcard pattern
        ``<prefix>_<entity>_*`` and returns a structured summary.

        Args:
            entity: Logical entity name.

        Returns:
            Dict with key ``"indices"`` containing a list of index stat
            dicts, or an empty dict on failure.
        """
        if self._client is None:
            return {}

        pattern = f"{self._prefix}_{entity}_*"
        try:
            response = await self._client.cat.indices(
                index=pattern,
                format="json",
                h="index,docs.count,store.size,health,status",
            )
            return {"entity": entity, "pattern": pattern, "indices": response}
        except Exception as exc:
            logger.warning("get_index_stats failed for entity %s: %s", entity, exc)
            return {}

    async def health_check(self) -> Dict[str, Any]:
        """Return a cluster health summary.

        Returns:
            A dict containing at minimum ``"status"`` (``"green"``,
            ``"yellow"``, or ``"red"``) and ``"indices"`` list.  Returns
            ``{"status": "unavailable"}`` when the client is ``None`` or the
            request fails.
        """
        if self._client is None:
            return {"status": "unavailable", "indices": [], "detail": "No Elasticsearch client configured."}

        try:
            health: Dict[str, Any] = await self._client.cluster.health()
            indices_resp = await self._client.cat.indices(format="json", h="index,health,status,docs.count")
            return {
                "status": health.get("status", "unknown"),
                "cluster_name": health.get("cluster_name", ""),
                "number_of_nodes": health.get("number_of_nodes", 0),
                "active_shards": health.get("active_shards", 0),
                "indices": indices_resp if isinstance(indices_resp, list) else [],
            }
        except Exception as exc:
            logger.warning("health_check failed: %s", exc)
            return {"status": "red", "indices": [], "error": str(exc)}

    # ------------------------------------------------------------------
    # Bootstrap
    # ------------------------------------------------------------------

    async def setup_default_indexes(self) -> None:
        """Create all indices declared in :data:`~app.core.elasticsearch_registry.ELASTICSEARCH_INDEXES`.

        Uses the sensible default mappings from :data:`_ENTITY_MAPPINGS` for
        known entity types.  Unknown entity types are skipped with a warning.

        This method is safe to call on every application startup because
        :meth:`create_index` is fully idempotent.
        """
        for table_name in ELASTICSEARCH_INDEXES:
            mappings = _ENTITY_MAPPINGS.get(table_name)
            if mappings is None:
                logger.warning(
                    "setup_default_indexes: no default mappings defined for entity %r — skipping.",
                    table_name,
                )
                continue
            success = await self.ensure_index(table_name, mappings)
            if success:
                logger.info("setup_default_indexes: index ready for entity %r.", table_name)
            else:
                logger.warning(
                    "setup_default_indexes: failed to ensure index for entity %r.", table_name
                )


# ---------------------------------------------------------------------------
# No-op fallback
# ---------------------------------------------------------------------------


class NullIndexManager(ElasticsearchIndexManager):
    """No-op index manager used when Elasticsearch is disabled or unavailable.

    Every method returns a safe empty/falsy value without making any network
    calls, so the application continues to operate normally without search.
    """

    def __init__(self) -> None:  # noqa: D107
        super().__init__(client=None, index_prefix="null")

    async def create_index(self, entity: str, mappings: Dict[str, Any], settings: Optional[Dict[str, Any]] = None, version: int = 1) -> bool:  # noqa: D102
        return False

    async def ensure_index(self, entity: str, mappings: Dict[str, Any]) -> bool:  # noqa: D102
        return False

    async def reindex(self, entity: str, new_mappings: Dict[str, Any], new_version: int) -> bool:  # noqa: D102
        return False

    async def delete_index(self, entity: str, version: int) -> bool:  # noqa: D102
        return False

    async def get_index_stats(self, entity: str) -> Dict[str, Any]:  # noqa: D102
        return {}

    async def health_check(self) -> Dict[str, Any]:  # noqa: D102
        return {"status": "disabled", "indices": []}

    async def setup_default_indexes(self) -> None:  # noqa: D102
        logger.debug("NullIndexManager.setup_default_indexes — no-op.")


# ---------------------------------------------------------------------------
# FastAPI dependency factory
# ---------------------------------------------------------------------------


def get_index_manager(request: Request) -> Union[ElasticsearchIndexManager, NullIndexManager]:
    """FastAPI dependency that resolves an :class:`ElasticsearchIndexManager`.

    Reads the Elasticsearch client from ``request.app.state.elasticsearch``
    (which may be a live :class:`~elasticsearch.AsyncElasticsearch` instance
    or a ``NullElasticsearch`` stub).

    If the client attribute is absent or the underlying ``client`` property is
    ``None``, a :class:`NullIndexManager` is returned so callers never receive
    ``None``.

    Args:
        request: The incoming FastAPI :class:`~fastapi.Request` object.

    Returns:
        An :class:`ElasticsearchIndexManager` or :class:`NullIndexManager`.
    """
    settings = get_settings()
    es_service = getattr(request.app.state, "elasticsearch", None)
    client = getattr(es_service, "client", None)

    if client is None or not _ES_AVAILABLE:
        return NullIndexManager()

    return ElasticsearchIndexManager(client, settings.ELASTICSEARCH_INDEX_PREFIX)


__all__ = [
    "ElasticsearchIndexManager",
    "NullIndexManager",
    "get_index_manager",
]
