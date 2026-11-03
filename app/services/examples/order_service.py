"""OrderService — Complete usage example of BaseService.

This module is DOCUMENTATION-QUALITY code showing how domain services are
built on top of :class:`~app.services.base.BaseService` using the Template
Method pattern.  It mirrors the structure of the production
:class:`~app.services.product.ProductService` while demonstrating every
hook and pattern a domain service can override or call.

Demonstrates:
    - ``BaseService.__init__`` wiring (repo, cache, elasticsearch, UnitOfWork).
    - ``before_create`` hook — validation and UUID pre-generation.
    - ``after_create`` hook — post-commit side effects (logging, notifications).
    - ``to_cache_dict`` hook — custom safe serialization for Redis.
    - ``to_read_schema`` hook — PII-masked or enriched public schema projection.
    - Custom service method calling the repository directly (``get_customer_orders``).
    - Elasticsearch search forwarded through the repository (``search_orders``).
    - Raw aggregation via :meth:`~app.core.elasticsearch.IElasticsearchService.search_raw`
      (``get_revenue_stats``).
    - Background job / cron-style bulk mutation (``cancel_stale_orders``).
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple
import uuid

from app.core.elasticsearch import IElasticsearchService
from app.core.exceptions import ConflictError, NotFoundError
from app.db.pool import UnitOfWork
from app.models.order import Order
from app.repositories.examples.order_repository import OrderRepository
from app.schemas.order import OrderCreate, OrderRead, OrderUpdate
from app.services.base import BaseService
from app.services.interface import ICacheService

logger = logging.getLogger(__name__)


class OrderService(BaseService[Order, OrderCreate, OrderUpdate, OrderRead, uuid.UUID]):
    """Domain service for the ``orders`` entity.

    Inherits the full CRUD template from :class:`~app.services.base.BaseService`
    (``create``, ``get``, ``update``, ``delete``, ``list_keyset``, ``list_offset``,
    ``search``) and overrides the lifecycle hooks to inject domain-specific
    behaviour.

    Dependency-injection pattern (mirrors ``ProductService``):

    .. code-block:: python

        # Via FastAPI Depends — constructed once per request:
        def get_order_service(
            db: UnitOfWork = Depends(get_db),
            cache: DynamicRedisCache = Depends(get_cache("orders")),
            es: ElasticsearchService = Depends(get_elasticsearch),
        ) -> OrderService:
            return OrderService(db=db, cache=cache, elasticsearch=es)
    """

    def __init__(
        self,
        db: Optional[UnitOfWork] = None,
        repo: Optional[OrderRepository] = None,
        cache: Optional[ICacheService] = None,
        elasticsearch: Optional[IElasticsearchService] = None,
        outbox_enabled: bool = False,
        auto_outbox_events: bool = False,
        outbox_topic: str = "domain.events",
        service_name: Optional[str] = None,
    ) -> None:
        resolved_repo: OrderRepository
        if repo is not None:
            resolved_repo = repo
        elif db is not None:
            # Thread cache and elasticsearch into the repository as well —
            # this gives ``BaseRepository``'s fetch_one/fetch_all/insert/…
            # methods access to Redis (for query-cache invalidation) and ES
            # (for outbox event staging), without requiring a second injection.
            resolved_repo = OrderRepository(db, cache=cache, elasticsearch=elasticsearch)
        else:
            raise ValueError("Either db session or repo must be provided to OrderService")

        super().__init__(
            repo=resolved_repo,
            read_schema_cls=OrderRead,
            cache=cache,
            elasticsearch=elasticsearch,
            order_col="created_at",  # keyset pagination sorts by this column
            id_col="id",
            db=db,
            outbox_enabled=outbox_enabled,
            auto_outbox_events=auto_outbox_events,
            outbox_topic=outbox_topic,
            service_name=service_name,
        )
        # Expose the concrete repo type for type-checked direct calls below
        self.repository: OrderRepository = resolved_repo

    # Convenience alias so callers can also reference the concrete type
    repository_class = OrderRepository

    # --------------------------------------------------------------------------
    # Lifecycle Hook 1: before_create — validation + UUID pre-generation
    # --------------------------------------------------------------------------

    async def before_create(self, payload: OrderCreate) -> Order:
        """Validates the payload and constructs the ``Order`` domain model.

        Override responsibilities:
        1. Generate the UUID *before* constructing the dataclass — it's a
           required positional field with no default (unlike ORM models that
           accept ``None`` and patch later).
        2. Run any uniqueness or business-rule checks that must happen before
           the INSERT (analogous to ``ProductService`` checking for a duplicate
           SKU here).

        Args:
            payload: Validated ``OrderCreate`` Pydantic DTO from the API layer.

        Returns:
            Fully-initialized ``Order`` dataclass ready for the repository.

        Raises:
            :class:`~app.core.exceptions.ConflictError`: If business rules are
                violated (e.g. duplicate payment reference — shown as example).
        """
        data: Dict[str, Any] = payload.model_dump()

        # Always generate the UUID here, never leave it for the DB SERIAL/IDENTITY
        # default — the entity ID may be needed for AAD binding in encryption or
        # for outbox event correlation before the INSERT round-trip completes.
        data["id"] = uuid.uuid4()

        # Example domain validation: enforce non-zero order amount
        if data.get("amount_cents", 0) <= 0:
            raise ConflictError("Order amount must be greater than zero cents.")

        # Construct the dataclass model directly — no ORM session involved.
        entity: Order = Order(**data)
        logger.debug("before_create: prepared Order id=%s for customer=%s", entity.id, entity.customer_id)
        return entity

    # --------------------------------------------------------------------------
    # Lifecycle Hook 2: after_create — post-commit side effects
    # --------------------------------------------------------------------------

    async def after_create(self, entity: Order) -> None:
        """Runs side effects after the order has been durably committed.

        This hook is invoked by :meth:`~app.services.base.BaseService.create`
        *after* ``self.db.commit()`` succeeds — so any exception raised here
        does NOT roll back the newly-created order (the row is already durable).

        Typical uses:
        - Send a confirmation email / push notification.
        - Enqueue a fulfillment job in Celery / ARQ.
        - Record an audit log entry in a separate write path.
        - Emit a telemetry span.

        Args:
            entity: The committed ``Order`` dataclass instance.
        """
        # In production, replace these stubs with real integrations:
        logger.info(
            "after_create: Order id=%s created for customer=%s, amount=%d cents",
            entity.id,
            entity.customer_id,
            entity.amount_cents,
        )
        # Example: send confirmation email (stubbed)
        # await self._email_client.send_order_confirmation(entity.customer_id, entity.id)

        # Example: dispatch a Celery task (stubbed)
        # fulfillment_queue.delay(str(entity.id))

    # --------------------------------------------------------------------------
    # Lifecycle Hook 3: to_cache_dict — custom safe serialization for Redis
    # --------------------------------------------------------------------------

    async def to_cache_dict(self, entity: Order) -> Dict[str, Any]:
        """Serializes an ``Order`` to the dict stored in Redis.

        The default ``BaseService.to_cache_dict`` delegates to ``to_read_schema``
        and calls ``model_dump(mode="json")``, which is correct for most entities.
        Override here when you need:
        - A different field set cached vs. what ``OrderRead`` exposes publicly.
        - Extra pre-computed fields (e.g. ``is_overdue``) that are cheap to cache
          but expensive to recompute per request.
        - Explicit exclusion of large blobs (e.g. line-item arrays) from the item
          cache to keep Redis memory usage bounded.

        Args:
            entity: ``Order`` dataclass instance to serialize.

        Returns:
            JSON-safe dict stored under ``cache:orders:item:<id>``.
        """
        return {
            "id": str(entity.id),
            "customer_id": str(entity.customer_id),
            "status": entity.status,
            "amount_cents": entity.amount_cents,
            "description": entity.description,
            "is_active": entity.is_active,
            # Serialize datetimes to ISO-8601 strings (orjson handles datetime
            # natively, but being explicit avoids surprises with timezone-naive values).
            "created_at": entity.created_at.isoformat() if entity.created_at else None,
            "updated_at": entity.updated_at.isoformat() if entity.updated_at else None,
            # Pre-compute a derived field so API handlers don't recompute it per-call
            "is_overdue": (
                entity.status == "pending"
                and entity.created_at is not None
                and entity.created_at < datetime.now(timezone.utc) - timedelta(days=7)
            ),
        }

    # --------------------------------------------------------------------------
    # Lifecycle Hook 4: to_read_schema — public schema projection
    # --------------------------------------------------------------------------

    async def to_read_schema(self, entity: Order) -> OrderRead:
        """Projects an ``Order`` dataclass to its public Pydantic read schema.

        Override ``BaseService.to_read_schema`` when you need to:
        - Decrypt or mask PII before returning (e.g. ``UserService`` masks phone numbers).
        - Enrich with computed fields not stored in the DB.
        - Raise ``NotFoundError`` for entities that have been soft-deleted and
          should not be surfaced to the API.

        The default implementation just calls
        ``self.read_schema_cls.model_validate(entity)``; this override adds a
        soft-delete guard as an example.

        Args:
            entity: ``Order`` dataclass instance from the repository.

        Returns:
            Validated ``OrderRead`` Pydantic model.

        Raises:
            :class:`~app.core.exceptions.NotFoundError`: If the order has been
                soft-deleted (``is_active=False``).
        """
        if not entity.is_active:
            # Soft-deleted orders should not be surfaced through the public API.
            # Raise NotFoundError so the service layer returns 404, not 200 with
            # an "inactive" order leaking through.
            raise NotFoundError(f"Order with ID '{entity.id}' not found")

        return OrderRead.model_validate(entity)

    # --------------------------------------------------------------------------
    # Custom Use Case 1: get_customer_orders — direct repo call with cache
    # --------------------------------------------------------------------------

    async def get_customer_orders(
        self,
        customer_id: uuid.UUID,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        """Returns all orders for a customer, served from the repository query cache.

        This bypasses the service-level item/list cache (``DynamicRedisCache.get_item``)
        and calls the repository directly — the repository's ``fetch_all`` with
        ``cache=True`` handles caching at the query level.

        This pattern is appropriate when:
        - The result set is a non-paginated list (not keyset-paginated).
        - The caller wants raw dicts rather than ``OrderRead`` schema instances.
        - The filter combination (``customer_id``) doesn't map to the generic
          ``list_keyset`` filter system the CRUDRouter generates.

        Args:
            customer_id: UUID of the customer whose orders to retrieve.
            limit: Maximum number of orders to return.

        Returns:
            List of raw order row dicts.
        """
        return await self.repository.get_orders_by_customer(customer_id, limit=limit)

    # --------------------------------------------------------------------------
    # Custom Use Case 2: search_orders — ES search via repo.search()
    # --------------------------------------------------------------------------

    async def search_orders(
        self,
        query: str,
        status: Optional[str] = None,
        limit: int = 20,
    ) -> Tuple[List[Dict[str, Any]], int]:
        """Full-text search over the Elasticsearch ``orders`` index (CQRS).

        The repository's :meth:`~app.repositories.examples.order_repository.OrderRepository.search_orders`
        builds the Elasticsearch DSL query and calls :meth:`~app.repositories.base.BaseRepository.search`,
        which delegates to :meth:`~app.core.elasticsearch.IElasticsearchService.search`.

        PostgreSQL is not touched — this is the CQRS read path.

        Args:
            query: Free-text search string.
            status: Optional status filter (exact match).
            limit: Maximum number of hits to return.

        Returns:
            ``(hits, total_count)`` tuple.  Each hit is a raw dict from ES.
        """
        return await self.repository.search_orders(
            search_text=query,
            status_filter=status,
            limit=limit,
        )

    # --------------------------------------------------------------------------
    # Custom Use Case 3: get_revenue_stats — ES aggregation example
    # --------------------------------------------------------------------------

    async def get_revenue_stats(
        self,
        start_date: datetime,
        end_date: datetime,
    ) -> Dict[str, Any]:
        """Returns revenue aggregation stats for a date range from Elasticsearch.

        Delegates to
        :meth:`~app.repositories.examples.order_repository.OrderRepository.get_revenue_aggregation`,
        which calls :meth:`~app.core.elasticsearch.IElasticsearchService.search_raw`
        with an Elasticsearch ``aggs`` body.

        This is the CQRS analytics path — Elasticsearch, not PostgreSQL, is the
        source of truth for aggregated read queries.

        Args:
            start_date: Inclusive range start (``gte`` on ``created_at``).
            end_date: Inclusive range end (``lte`` on ``created_at``).

        Returns:
            Dict with ``total_revenue_cents``, ``order_count``, ``start_date``,
            and ``end_date`` keys.  Returns zeros when Elasticsearch is unavailable.
        """
        return await self.repository.get_revenue_aggregation(
            start_date=start_date,
            end_date=end_date,
        )

    # --------------------------------------------------------------------------
    # Custom Use Case 4: cancel_stale_orders — background job / cron pattern
    # --------------------------------------------------------------------------

    async def cancel_stale_orders(
        self,
        max_age_days: int = 7,
    ) -> str:
        """Bulk-cancels all pending orders older than ``max_age_days`` days.

        Designed to be called from a background job scheduler (e.g. APScheduler,
        ARQ, Celery beat) rather than an HTTP endpoint.  The service owns the
        transaction boundary: it calls ``self.db.commit()`` after the mutation
        and then logs the result.

        The repository's :meth:`~app.repositories.examples.order_repository.OrderRepository.cancel_unpaid_orders_older_than`
        method runs a bulk ``UPDATE`` via ``execute()`` (not ``update_row()``, which
        only handles single-row mutations by primary key) — see the repository
        docstring for why ``auto_sync=False`` is used there.

        Args:
            max_age_days: Orders older than this many days are cancelled.

        Returns:
            asyncpg status string from the underlying UPDATE (e.g. ``"UPDATE 5"``).
        """
        cutoff = datetime.now(timezone.utc) - timedelta(days=max_age_days)
        status = await self.repository.cancel_unpaid_orders_older_than(cutoff)

        # Commit the bulk update — service holds the transaction boundary
        if self.db is not None:
            await self.db.commit()

        # Invalidate the list cache after a bulk mutation so subsequent
        # list_keyset / get_customer_orders calls recompute from PostgreSQL.
        await self.cache.invalidate_lists()

        logger.info("cancel_stale_orders: %s (cutoff=%s)", status, cutoff.date())
        return status


__all__ = ["OrderService"]
