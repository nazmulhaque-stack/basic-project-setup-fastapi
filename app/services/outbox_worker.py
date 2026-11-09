"""Transactional Outbox Background Auto-Sync Worker.

Continuously polls the PostgreSQL `outbox` table, delivers documents to Elasticsearch,
and updates `published_at` and `status` to track Elasticsearch synchronization.
Uses `FOR UPDATE SKIP LOCKED` for concurrent multi-worker safety across cluster nodes.
"""

import asyncio
from datetime import datetime, timezone
import logging
from typing import Any, Dict, List, Optional
import uuid

import asyncpg
import orjson

from app.core.elasticsearch import IElasticsearchService
from app.db.pool import UnitOfWork
from app.models.outbox import OutboxEvent
from app.repositories.outbox import OutboxRepository

logger = logging.getLogger("app.services.outbox_worker")


class OutboxWorker:
    """Asynchronous background worker that auto-syncs outbox records to Elasticsearch."""

    def __init__(
        self,
        db_pool: asyncpg.Pool,
        elasticsearch: IElasticsearchService,
        poll_interval: float = 1.0,
        batch_size: int = 50,
        max_retries: int = 5,
    ) -> None:
        self.db_pool = db_pool
        self.es = elasticsearch
        self.poll_interval = poll_interval
        self.batch_size = batch_size
        self.max_retries = max_retries
        self._running: bool = False
        self._task: Optional[asyncio.Task[None]] = None

    def start(self) -> None:
        """Starts the outbox background polling loop as an asyncio Task."""
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._run_loop(), name="outbox-auto-sync-worker")
        logger.info(
            "Transactional Outbox auto-sync worker started (poll_interval=%.1fs, batch_size=%d).",
            self.poll_interval,
            self.batch_size,
        )

    async def stop(self) -> None:
        """Gracefully stops the outbox background polling loop."""
        self._running = False
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("Transactional Outbox auto-sync worker stopped.")

    async def _run_loop(self) -> None:
        """Continuous polling loop."""
        while self._running:
            try:
                processed_count = await self.process_batch()
                # If we processed a full batch, check immediately for more without delay
                if processed_count < self.batch_size:
                    await asyncio.sleep(self.poll_interval)
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error("Unexpected error in Outbox auto-sync worker loop: %s", exc, exc_info=True)
                await asyncio.sleep(self.poll_interval)

    async def _sync_single_event_to_es(self, event: OutboxEvent) -> None:
        """Dispatches an outbox event to the appropriate Elasticsearch index."""
        agg_type = event.aggregate_type.lower()
        agg_id = str(event.aggregate_id)
        event_type = event.event_type.upper()
        payload = event.payload or {}

        # Safe decoding if payload was double-encoded or stored as string
        if isinstance(payload, str):
            try:
                parsed = orjson.loads(payload)
                if isinstance(parsed, str):
                    parsed = orjson.loads(parsed)
                payload = parsed if isinstance(parsed, dict) else {}
            except Exception:
                payload = {}

        if agg_type in ("user", "user_profile"):
            if event_type in ("CREATE", "UPDATE", "ACCOUNT_ADDED"):
                # 1. Sync User index document: handles both direct doc or nested {"user": {...}}
                user_doc = payload.get("user") if isinstance(payload, dict) and "user" in payload else payload
                if isinstance(user_doc, str):
                    try:
                        user_doc = orjson.loads(user_doc)
                    except Exception:
                        user_doc = {}

                if isinstance(user_doc, dict) and user_doc:
                    await self.es.index_document(index="user", id_=agg_id, document=user_doc)

                # 2. Sync UserProfile index document if present
                profile_doc = payload.get("profile") if isinstance(payload, dict) else None
                if isinstance(profile_doc, dict) and profile_doc:
                    await self.es.index_document(index="user_profile", id_=agg_id, document=profile_doc)

            elif event_type == "DELETE":
                await self.es.delete_document(index="user", id_=agg_id)
                await self.es.delete_document(index="user_profile", id_=agg_id)
        else:
            if event_type in ("CREATE", "UPDATE"):
                if isinstance(payload, dict) and payload:
                    await self.es.index_document(index=agg_type, id_=agg_id, document=payload)
            elif event_type == "DELETE":
                await self.es.delete_document(index=agg_type, id_=agg_id)

    async def process_batch(self) -> int:
        """Processes one batch of pending outbox events using FOR UPDATE SKIP LOCKED."""
        async with self.db_pool.acquire() as conn:
            async with conn.transaction():
                uow = UnitOfWork(conn)
                repo = OutboxRepository(uow)

                events: List[OutboxEvent] = await repo.fetch_pending_batch(limit=self.batch_size)
                if not events:
                    return 0

                for event in events:
                    now = datetime.now(timezone.utc)
                    try:
                        await self._sync_single_event_to_es(event)
                        # Mark published upon verified Elasticsearch delivery
                        await repo.mark_published(event.id, published_at=now)
                        logger.info(
                            "Outbox event %s (%s:%s [%s]) successfully synced to Elasticsearch.",
                            event.id,
                            event.aggregate_type,
                            event.aggregate_id,
                            event.event_type,
                        )
                    except Exception as exc:
                        error_msg = str(exc)
                        await repo.mark_failed(event.id, error_message=error_msg, max_retries=self.max_retries)
                        logger.warning(
                            "Failed to sync outbox event %s to Elasticsearch (attempt %d): %s",
                            event.id,
                            event.retry_count + 1,
                            error_msg,
                        )

                return len(events)

    async def sync_now(self, event_id: uuid.UUID) -> bool:
        """Directly syncs an outbox event immediately after transaction commit."""
        async with self.db_pool.acquire() as conn:
            async with conn.transaction():
                uow = UnitOfWork(conn)
                repo = OutboxRepository(uow)
                event = await repo.get(event_id)
                if event is None or event.status == "PUBLISHED":
                    return True

                now = datetime.now(timezone.utc)
                try:
                    await self._sync_single_event_to_es(event)
                    await repo.mark_published(event.id, published_at=now)
                    return True
                except Exception as exc:
                    await repo.mark_failed(event.id, error_message=str(exc), max_retries=self.max_retries)
                    return False


__all__ = ["OutboxWorker"]
