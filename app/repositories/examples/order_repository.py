"""OrderRepository — Complete usage example of BaseRepository.

This module is DOCUMENTATION-QUALITY code showing every pattern that concrete
repositories can use from ``BaseRepository``. It is intentionally verbose and
annotated — production repositories (e.g. ``ProductRepository``) follow the
same patterns but without the tutorial commentary.

Demonstrates:
    1.  ``fetch_one`` with ``cache=True`` / ``cache=False``
    2.  ``fetch_all`` with filtering
    3.  ``insert`` with ``auto_sync=True``
    4.  ``update_row`` with ``SQLExpr("now()")``
    5.  ``delete_row`` — hard delete and soft delete
    6.  ``execute`` for complex conditional UPDATE
    7.  ``execute_many`` for bulk inserts
    8.  ``bulk_sync`` for explicit ES sync after ``execute_many``
    9.  ``search()`` for Elasticsearch DSL queries
    10. ``search_enabled=False`` short-circuit pattern
    11. Custom aggregations via ``elasticsearch.search_raw``
    12. ``transaction()`` for nested savepoints (partial commit/rollback)
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
import uuid

from app.models.order import Order
from app.repositories.base import BaseRepository, SQLExpr

logger = logging.getLogger(__name__)


class OrderRepository(BaseRepository[Order, uuid.UUID]):
    """Concrete repository for the (example) ``orders`` table.

    Inherits all caching, Elasticsearch sync, and execution machinery from
    :class:`~app.repositories.base.BaseRepository`.  Only entity-specific SQL
    and filter logic live here — nothing generic is re-implemented.

    Class Attributes:
        table: The PostgreSQL table name consumed by ``insert`` / ``update_row``
            / ``delete_row`` / ``execute`` helpers.
        primary_key: Primary-key column used for cache invalidation keys and
            auto Elasticsearch sync event routing.
        cache_prefix: Namespace prefix for all Redis keys this repository
            manages (``cache:orders:…``).  Defaults to ``table`` when omitted.
    """

    table = "orders"
    primary_key = "id"
    cache_prefix = "orders"

    @property
    def model(self) -> type[Order]:
        """Exposes the domain model class (required by ``BaseRepository`` introspection)."""
        return Order

    # ------------------------------------------------------------------
    # 1. fetch_one — cache=True (cached read)
    # ------------------------------------------------------------------

    async def get_order(self, order_id: uuid.UUID) -> Optional[Dict[str, Any]]:
        """Fetches a single order by primary key, served from cache when warm.

        ``cache=True`` stores the (query, params) pair under the repository's
        versioned query-cache namespace (``cache:orders:query:v<N>:<hash>``)
        for ``cache_ttl`` seconds.  Any subsequent ``insert`` / ``update_row``
        / ``delete_row`` / ``execute`` call atomically bumps the version
        counter, orphaning the cached key in O(1) without a costly ``SCAN``.

        Args:
            order_id: UUID primary key of the order to retrieve.

        Returns:
            Row dict if found, ``None`` otherwise.
        """
        return await self.fetch_one(
            "SELECT * FROM orders WHERE id = $1",
            (order_id,),
            cache=True,
            cache_ttl=300,  # 5 minutes — reasonable for a read-heavy entity
        )

    # ------------------------------------------------------------------
    # 2. fetch_one — cache=False (bypass Redis for post-mutation reads)
    # ------------------------------------------------------------------

    async def get_order_no_cache(self, order_id: uuid.UUID) -> Optional[Dict[str, Any]]:
        """Fetches an order directly from PostgreSQL, bypassing Redis entirely.

        Use this immediately after a mutation to get the freshly-committed row
        without risk of returning a stale cached value.  This is the canonical
        "post-mutation consistency check" pattern — the service layer calls
        ``cache_enabled=False`` on the *service*-level item cache, while this
        method bypasses the *repository*-level query cache.

        .. code-block:: python

            # Typical usage inside a service method
            order = await self.repo.update_order_status(order_id, "shipped")
            # Immediately re-read with no cache to confirm the write landed:
            fresh = await self.repo.get_order_no_cache(order_id)

        Args:
            order_id: UUID primary key of the order to retrieve.

        Returns:
            Row dict if found, ``None`` otherwise.
        """
        # cache=False (the default) → skips both the cache read AND the cache write.
        return await self.fetch_one(
            "SELECT * FROM orders WHERE id = $1",
            (order_id,),
            cache=False,  # Post-mutation consistency check — never serve stale data here
        )

    # ------------------------------------------------------------------
    # 3. fetch_all — filtered list with cache
    # ------------------------------------------------------------------

    async def get_orders_by_customer(
        self,
        customer_id: uuid.UUID,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        """Returns all orders belonging to a customer, most-recent-first.

        ``cache=True`` caches the full result list under the repository's
        versioned namespace — the same version bump that invalidates
        ``get_order``'s per-row cache also invalidates these list results,
        so there is never a window where the list shows an order that a
        concurrent ``create_order`` or ``cancel_unpaid_orders_older_than``
        has already modified.

        Args:
            customer_id: UUID of the customer whose orders to fetch.
            limit: Maximum number of rows to return (caller should enforce ≤ 100).

        Returns:
            List of row dicts ordered by ``created_at DESC``.
        """
        return await self.fetch_all(
            "SELECT * FROM orders WHERE customer_id = $1 ORDER BY created_at DESC LIMIT $2",
            (customer_id, limit),
            cache=True,
            cache_ttl=120,  # 2 minutes — list caches are shorter-lived than item caches
        )

    # ------------------------------------------------------------------
    # 4. insert — with auto_sync=True (default)
    # ------------------------------------------------------------------

    async def create_order(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Inserts a new order row and automatically enqueues an ES sync event.

        ``insert()`` builds a fully-parameterized ``INSERT … RETURNING *``
        from ``data``'s keys/values, then:
        a. Atomically bumps the query-cache version (O(1) list invalidation).
        b. With ``auto_sync=True`` (the default), stages an ``es_sync.orders.upsert``
           row in ``outbox_events`` within the **same, still-open transaction** —
           :class:`~app.services.elasticsearch_sync.ElasticsearchSyncDispatcher`
           will pick it up after ``commit()``, so Elasticsearch is never
           written inside a live database transaction.

        .. code-block:: python

            order = await repo.create_order({
                "id": uuid.uuid4(),
                "customer_id": customer_id,
                "status": "pending",
                "amount_cents": 4999,
                "created_at": SQLExpr("now()"),  # let Postgres set the timestamp
            })

        Args:
            data: Column→value mapping for the new row.  Values may be plain
                Python scalars or :class:`~app.repositories.base.SQLExpr`
                instances (e.g. ``SQLExpr("now()")``) for SQL-side expressions.

        Returns:
            The newly-inserted row as a dict (from ``RETURNING *``).
        """
        return await self.insert(
            data,
            auto_sync=True,  # enqueue ES outbox event inside the same transaction
        )

    # ------------------------------------------------------------------
    # 5. update_row — with SQLExpr("now()") for server-side timestamp
    # ------------------------------------------------------------------

    async def update_order_status(
        self,
        order_id: uuid.UUID,
        status: str,
    ) -> Optional[Dict[str, Any]]:
        """Updates an order's status column and refreshes ``updated_at`` server-side.

        Using ``SQLExpr("now()")`` for ``updated_at`` tells ``update_row()``
        to inline the expression literally (``updated_at = now()``) rather
        than binding a Python ``datetime`` — this avoids any clock-skew
        between the application server and the database and is the idiomatic
        asyncpg pattern for ``timestamptz`` columns that Postgres should own.

        **Never** pass request-controlled input into ``SQLExpr`` — only use it
        with hard-coded SQL fragments authored by the developer.

        Args:
            order_id: UUID primary key of the order to update.
            status: New status string (e.g. ``"shipped"``, ``"cancelled"``).

        Returns:
            Updated row dict, or ``None`` if no order with that ID exists.
        """
        return await self.update_row(
            {
                "status": status,
                "updated_at": SQLExpr("now()"),  # server-side timestamp — no clock drift
            },
            where={"id": order_id},
            auto_sync=True,  # re-indexes the updated document in Elasticsearch
        )

    # ------------------------------------------------------------------
    # 6a. delete_row — soft delete (flag inactive, keep row)
    # ------------------------------------------------------------------

    async def soft_delete_order(self, order_id: uuid.UUID) -> int:
        """Soft-deletes an order by setting ``is_active = false``.

        A soft delete does **not** remove the row from PostgreSQL — it flips
        the ``is_active`` flag to ``false`` and then re-syncs the document to
        Elasticsearch (``upsert`` op, not ``delete``), so the search index
        reflects the updated state (e.g. search can filter on ``is_active``).

        ``delete_row`` returns the number of rows affected, which is 0 when
        no row with that ``id`` existed.

        Args:
            order_id: UUID primary key of the order to soft-delete.

        Returns:
            1 if the order was found and flagged inactive, 0 if not found.
        """
        return await self.delete_row(
            where={"id": order_id},
            soft_delete=True,
            soft_delete_column="is_active",   # column set to ``false``
            auto_sync=True,                   # re-index as inactive in ES
        )

    # ------------------------------------------------------------------
    # 6b. delete_row — hard delete (remove row permanently)
    # ------------------------------------------------------------------

    async def hard_delete_order(self, order_id: uuid.UUID) -> int:
        """Permanently removes an order row from PostgreSQL.

        With ``soft_delete=False`` (the default), ``delete_row`` executes
        ``DELETE … RETURNING id`` and enqueues an ``es_sync.orders.delete``
        outbox event so the document is removed from the Elasticsearch index
        after commit.

        Args:
            order_id: UUID primary key of the order to permanently delete.

        Returns:
            1 if the order was found and deleted, 0 if not found.
        """
        return await self.delete_row(
            where={"id": order_id},
            auto_sync=True,  # enqueue ES 'delete' outbox event inside same transaction
        )

    # ------------------------------------------------------------------
    # 7. execute — complex conditional UPDATE (no RETURNING *)
    # ------------------------------------------------------------------

    async def cancel_unpaid_orders_older_than(self, cutoff_datetime: datetime) -> str:
        """Bulk-cancels stale pending orders older than a given cutoff timestamp.

        ``execute()`` is for arbitrary write SQL that ``insert`` / ``update_row``
        / ``delete_row`` cannot express — here: a multi-row UPDATE with no
        ``RETURNING`` clause, touching rows we cannot individually enumerate.

        Because we don't know the exact IDs affected, we pass ``auto_sync=False``
        and ``entity_id=None`` — there's no safe, generic way to infer which
        rows changed from the SQL string alone.  If Elasticsearch consistency is
        required, the caller should schedule a full re-index of the ``orders``
        index, or use a separate follow-up query to identify the changed IDs and
        call ``bulk_sync`` on them.

        ``execute()`` always atomically bumps the query-cache version key, so
        subsequent ``get_orders_by_customer`` / ``get_order`` calls will
        recompute from PostgreSQL rather than returning stale cached data.

        Args:
            cutoff_datetime: Orders with ``created_at < cutoff_datetime`` and
                ``status = 'pending'`` are transitioned to ``'cancelled'``.

        Returns:
            asyncpg status string, e.g. ``"UPDATE 42"``.
        """
        status: str = await self.execute(
            """
            UPDATE orders
               SET status      = 'cancelled',
                   updated_at  = now()
             WHERE status      = 'pending'
               AND created_at  < $1
            """,
            (cutoff_datetime,),
            auto_sync=False,  # no single entity_id available for a multi-row UPDATE
        )
        row_count = status.split()[-1] if status else "0"
        logger.info("cancel_unpaid_orders_older_than: cancelled %s order(s)", row_count)
        return status

    # ------------------------------------------------------------------
    # 8. execute_many + bulk_sync — bulk insert + explicit ES sync
    # ------------------------------------------------------------------

    async def bulk_create_order_items(
        self,
        items: List[Tuple[uuid.UUID, uuid.UUID, int, int]],
    ) -> None:
        """Bulk-inserts order-item rows and then explicitly syncs them to ES.

        ``execute_many`` executes the INSERT once per tuple in ``items`` in a
        single asyncpg network round-trip (``executemany``).  It **always**
        invalidates the query-cache version but does **not** auto-sync to
        Elasticsearch — a bulk write's affected IDs aren't known generically
        from the statement string alone.

        Call ``bulk_sync`` immediately afterward (within the same transaction,
        before ``commit()``) to stage outbox events for each affected ID so
        the ``ElasticsearchSyncDispatcher`` picks them up post-commit.

        Args:
            items: List of ``(order_id, product_id, quantity, price_cents)``
                tuples to insert into ``order_items``.
        """
        await self.execute_many(
            """
            INSERT INTO order_items (order_id, product_id, quantity, price_cents)
            VALUES ($1, $2, $3, $4)
            """,
            items,
        )
        # Explicitly enqueue one ES outbox event per affected order so the
        # Elasticsearch index reflects each order's updated line items.
        # ``bulk_sync("upsert", ids)`` calls ``_enqueue_es_sync`` per ID,
        # staging the events in the same still-open transaction.
        order_ids: List[uuid.UUID] = [item[0] for item in items]
        await self.bulk_sync("upsert", order_ids)
        logger.debug("bulk_create_order_items: queued ES sync for %d order(s)", len(order_ids))

    # ------------------------------------------------------------------
    # 9. search — Elasticsearch DSL query (search_enabled=True, default)
    # ------------------------------------------------------------------

    async def search_orders(
        self,
        search_text: str,
        status_filter: Optional[str] = None,
        limit: int = 20,
    ) -> Tuple[List[Dict[str, Any]], int]:
        """Full-text search over the ``orders`` Elasticsearch index.

        Delegates to :meth:`~app.repositories.base.BaseRepository.search`,
        which calls ``IElasticsearchService.search`` and returns
        ``(hits, total)``.  The index name is resolved automatically from
        :data:`~app.core.elasticsearch_registry.ELASTICSEARCH_INDEXES`
        (keyed by ``self.table``).

        The ``bool`` DSL query here combines:
        - ``must`` → full-text match on ``description`` (relevance scoring).
        - ``filter`` → exact ``status`` term when provided (zero-scoring filter).

        Args:
            search_text: Free-text query string matched against ``description``.
            status_filter: Optional exact-match filter on the ``status`` field.
            limit: Maximum number of Elasticsearch hits to return.

        Returns:
            ``(hits, total_count)`` tuple where each hit is a plain dict.
        """
        dsl_query: Dict[str, Any] = {
            "bool": {
                "must": [
                    {"match": {"description": search_text}},
                ],
                "filter": (
                    [{"term": {"status": status_filter}}]
                    if status_filter
                    else []
                ),
            }
        }
        return await self.search(
            dsl_query,
            search_enabled=True,  # True is the default; shown explicitly for clarity
            limit=limit,
        )

    # ------------------------------------------------------------------
    # 10. search — search_enabled=False (skip ES, fall back to DB read)
    # ------------------------------------------------------------------

    async def search_orders_disabled(self, order_id: uuid.UUID) -> Optional[Dict[str, Any]]:
        """Demonstrates the ``search_enabled=False`` short-circuit pattern.

        When ``search_enabled=False``, :meth:`~app.repositories.base.BaseRepository.search`
        returns ``([], 0)`` immediately without touching the Elasticsearch cluster.
        This is useful when:
        - The caller already knows ES is unavailable for a particular request.
        - A feature flag disables search for a specific tenant.
        - A test wants to assert the repository path without an ES mock.

        After the short-circuit, we fall back to a direct database read.

        Args:
            order_id: UUID of the specific order to fetch as a fallback.

        Returns:
            Row dict if found, ``None`` otherwise.
        """
        # search_enabled=False → returns ([], 0) instantly, no ES call made
        hits, _total = await self.search(
            {"term": {"id": str(order_id)}},
            search_enabled=False,
        )
        if hits:
            return hits[0]

        # Fallback: read directly from PostgreSQL when search is disabled
        return await self.fetch_one(
            "SELECT * FROM orders WHERE id = $1",
            (order_id,),
            cache=False,  # bypass cache — this is a consistency-sensitive read
        )

    # ------------------------------------------------------------------
    # 11. Custom aggregations via elasticsearch.search_raw
    # ------------------------------------------------------------------

    async def get_revenue_aggregation(
        self,
        start_date: datetime,
        end_date: datetime,
    ) -> Dict[str, Any]:
        """Runs a revenue aggregation query directly against Elasticsearch.

        :meth:`~app.core.elasticsearch.IElasticsearchService.search_raw`
        lets repositories submit **any** valid Elasticsearch request body and
        get back the raw response dict — useful for ``aggs``, ``suggest``,
        ``_source`` filtering, or other DSL features that the generic
        ``search()`` wrapper doesn't expose.

        The aggregation body here computes:
        - ``total_revenue``: Sum of ``amount_cents`` across matched orders.
        - ``order_count``: Count of distinct ``id`` values (i.e. order count).

        Args:
            start_date: Inclusive lower bound for ``created_at`` range filter.
            end_date: Inclusive upper bound for ``created_at`` range filter.

        Returns:
            Raw Elasticsearch response dict, including ``aggregations`` key.
            Returns an empty dict when Elasticsearch is disabled (``NullElasticsearch``).

        Example return shape::

            {
                "hits": {"total": {"value": 0}, "hits": []},
                "aggregations": {
                    "total_revenue": {"value": 1234567},
                    "order_count":   {"value": 42},
                },
            }
        """
        from app.core.elasticsearch_registry import get_index_config

        cfg = get_index_config(self.table)
        index = cfg["index"] if cfg else self.table

        body: Dict[str, Any] = {
            "size": 0,  # we only want aggregation results, not document hits
            "query": {
                "range": {
                    "created_at": {
                        "gte": start_date.isoformat(),
                        "lte": end_date.isoformat(),
                    }
                }
            },
            "aggs": {
                "total_revenue": {
                    "sum": {"field": "amount_cents"}
                },
                "order_count": {
                    "value_count": {"field": "id"}
                },
            },
        }

        try:
            response = await self.elasticsearch.search_raw(index=index, body=body)
        except Exception:
            logger.exception("get_revenue_aggregation: ES aggregation failed, returning empty")
            return {}

        aggs = response.get("aggregations", {})
        return {
            "total_revenue_cents": aggs.get("total_revenue", {}).get("value", 0),
            "order_count": aggs.get("order_count", {}).get("value", 0),
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
        }

    # ------------------------------------------------------------------
    # 12. transaction() — nested savepoint for partial commit/rollback
    # ------------------------------------------------------------------

    async def atomic_payment_flow(
        self,
        order_id: uuid.UUID,
        payment_data: Dict[str, Any],
    ) -> bool:
        """Demonstrates the nested savepoint pattern using ``self.transaction()``.

        Most callers never need this — the outer ``UnitOfWork`` transaction
        opened by ``get_db`` already covers an entire HTTP request.  Use
        ``transaction()`` when a **service method** needs to try a sub-step,
        and if that sub-step fails, roll back only the sub-step (back to the
        savepoint) while keeping earlier writes in the same request durable.

        Concretely here: we attempt to update the order status to ``"paid"``
        and upsert a payment record inside a nested savepoint.  If the payment
        insert fails (e.g. duplicate payment reference), we roll back to the
        savepoint — the order row update is also rolled back to its pre-savepoint
        state — but any writes the service made *before* calling this method
        (e.g. an audit log row) are untouched.

        .. warning::
            asyncpg implements nested :meth:`connection.transaction()` calls
            as ``SAVEPOINT`` / ``ROLLBACK TO SAVEPOINT`` automatically (not
            full ``BEGIN`` / ``ROLLBACK``), because the outer transaction is
            already open.  Never call :meth:`UnitOfWork.commit` or
            :meth:`UnitOfWork.rollback` from inside a repository method — only
            services hold transaction boundaries.

        Args:
            order_id: UUID of the order being paid.
            payment_data: Dict containing payment reference details to record.

        Returns:
            ``True`` if the payment flow committed successfully,
            ``False`` if the nested savepoint was rolled back.
        """
        async with self.transaction():  # opens a SAVEPOINT
            try:
                # Step 1: flip order status to "paid" (UPDATE … RETURNING *)
                updated = await self.update_row(
                    {"status": "paid", "updated_at": SQLExpr("now()")},
                    where={"id": order_id},
                    auto_sync=False,  # we'll sync once at the end if everything succeeds
                )
                if updated is None:
                    # Order not found — roll back savepoint, signal failure
                    logger.warning("atomic_payment_flow: order %s not found", order_id)
                    return False

                # Step 2: insert payment record (may raise on duplicate reference)
                await self.insert(
                    {
                        "id": payment_data.get("id", uuid.uuid4()),
                        "order_id": order_id,
                        "reference": payment_data["reference"],
                        "amount_cents": payment_data["amount_cents"],
                        "created_at": SQLExpr("now()"),
                    },
                    table="payments",       # target a different table
                    auto_sync=False,        # payments aren't in ELASTICSEARCH_INDEXES
                )

                # Step 3: enqueue one ES sync for the updated order
                await self.bulk_sync("upsert", [order_id])

                logger.info("atomic_payment_flow: payment committed for order %s", order_id)
                return True

            except Exception:
                # The ``async with self.transaction()`` context manager issues
                # ``ROLLBACK TO SAVEPOINT`` automatically on exception exit —
                # we just log and propagate the False return.
                logger.exception(
                    "atomic_payment_flow: savepoint rolled back for order %s", order_id
                )
                return False


__all__ = ["OrderRepository"]
