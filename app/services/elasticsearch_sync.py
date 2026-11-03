"""Background dispatcher applying outbox-staged Elasticsearch sync events.

`BaseRepository.insert`/`update_row`/`delete_row` (in `app/repositories/base.py`)
stage a row in `outbox_events` (topic `"elasticsearch.sync"`) in the SAME
transaction as the database write, for every table registered in
`app.core.elasticsearch_registry.ELASTICSEARCH_INDEXES`. This dispatcher is
the only thing that actually talks to Elasticsearch for those writes — never
the request path itself — so a slow or momentarily unreachable cluster never
blocks or fails an API request, and a crash between "wrote to Postgres" and
"indexed in Elasticsearch" just means the sync event is picked up on the next
poll instead of being lost.

Structurally this mirrors `app.services.outbox.OutboxDispatcher` (same
`FOR UPDATE SKIP LOCKED` + strict per-aggregate FIFO ordering, so a delete can
never be applied before an earlier-queued update for the same row), but
drives the Elasticsearch client instead of a Kafka producer, and reads the
`index`/`primary_key` straight out of each event's own payload rather than
needing a producer object at all.
"""

import asyncio
from datetime import datetime, timezone
import logging
from typing import Optional

import asyncpg

from app.core.elasticsearch import IElasticsearchService
from app.repositories.base import ES_SYNC_TOPIC

logger = logging.getLogger(__name__)

# Same FIFO-per-aggregate guarantee as the Kafka outbox dispatcher, scoped to
# this dedicated topic so the two dispatchers never contend for the same rows.
_SELECT_PENDING_ES_SQL = f"""
SELECT * FROM outbox_events AS o
WHERE o.topic = '{ES_SYNC_TOPIC}'
  AND o.published_at IS NULL
  AND NOT EXISTS (
      SELECT 1 FROM outbox_events AS older
      WHERE older.topic = o.topic
        AND older.event_key = o.event_key
        AND older.published_at IS NULL
        AND (
            older.created_at < o.created_at
            OR (older.created_at = o.created_at AND older.id < o.id)
        )
  )
ORDER BY o.created_at, o.id
LIMIT $1
FOR UPDATE SKIP LOCKED
"""

# After this many failed attempts, an event is dead-lettered (marked
# published so it stops blocking later events for the same row) rather than
# retried forever — a single poison event (e.g. a document that always fails
# mapping validation) must not permanently stall sync for that aggregate.
DEFAULT_MAX_ATTEMPTS = 10


class ElasticsearchSyncDispatcher:
    """Applies committed Elasticsearch sync events from the outbox to the cluster."""

    def __init__(
        self,
        pool: asyncpg.Pool,
        elasticsearch: IElasticsearchService,
        *,
        poll_interval: float = 1.0,
        batch_size: int = 100,
        max_attempts: int = DEFAULT_MAX_ATTEMPTS,
    ) -> None:
        self._pool = pool
        self._es = elasticsearch
        self._poll_interval = poll_interval
        self._batch_size = batch_size
        self._max_attempts = max_attempts
        self._task: Optional[asyncio.Task[None]] = None

    def start(self) -> None:
        if self._task is not None:
            raise RuntimeError("Elasticsearch sync dispatcher is already running.")
        self._task = asyncio.create_task(self._run(), name="elasticsearch-sync-dispatcher")

    async def stop(self) -> None:
        if self._task is None:
            return
        self._task.cancel()
        try:
            await self._task
        except asyncio.CancelledError:
            pass
        finally:
            self._task = None

    async def _apply(self, payload: dict) -> None:
        """Applies one sync event's operation to Elasticsearch. Raises on
        failure so the caller can record the attempt/error and retry."""
        op = payload["op"]
        index = payload["index"]
        entity_id = payload["entity_id"]
        data = payload.get("data")

        if op == "delete":
            ok = await self._es.delete_document(index=index, id_=entity_id)
        elif data is not None:
            ok = await self._es.index_document(index=index, id_=entity_id, document=data)
        else:
            # An "upsert" staged without the row's data (e.g. a conditional
            # UPDATE that only returns a status string) has nothing to index
            # with — there's no safe generic way to re-fetch it here without
            # knowing the table's columns, so skip it; the next read-driven
            # cache refresh or an explicit reindex covers staleness.
            logger.warning(
                "Skipping Elasticsearch sync for %s/%s: upsert staged with no document data.",
                index,
                entity_id,
            )
            return

        if not ok:
            raise RuntimeError(f"Elasticsearch {op} failed for {index}/{entity_id}")

    async def dispatch_once(self) -> int:
        """Applies one batch and marks successful rows in the same DB transaction."""
        applied = 0
        async with self._pool.acquire() as connection:
            async with connection.transaction():
                rows = await connection.fetch(_SELECT_PENDING_ES_SQL, self._batch_size)
                for row in rows:
                    try:
                        await self._apply(row["payload"])
                    except Exception as exc:
                        attempts = row["attempts"] + 1
                        if attempts >= self._max_attempts:
                            logger.critical(
                                "Dead-lettering Elasticsearch sync event %s after %d attempts: %s",
                                row["id"],
                                attempts,
                                exc,
                            )
                            await connection.execute(
                                """
                                UPDATE outbox_events
                                SET attempts = $2, last_error = $3, published_at = $4
                                WHERE id = $1
                                """,
                                row["id"],
                                attempts,
                                str(exc)[:4000],
                                datetime.now(timezone.utc),
                            )
                        else:
                            await connection.execute(
                                "UPDATE outbox_events SET attempts = $2, last_error = $3 WHERE id = $1",
                                row["id"],
                                attempts,
                                str(exc)[:4000],
                            )
                            logger.warning(
                                "Elasticsearch sync failed for event %s (attempt %d/%d): %s",
                                row["id"],
                                attempts,
                                self._max_attempts,
                                exc,
                            )
                        continue

                    await connection.execute(
                        "UPDATE outbox_events SET published_at = $2, last_error = NULL WHERE id = $1",
                        row["id"],
                        datetime.now(timezone.utc),
                    )
                    applied += 1
        return applied

    async def _run(self) -> None:
        while True:
            try:
                applied = await self.dispatch_once()
                if applied:
                    logger.info("Applied %d Elasticsearch sync event(s).", applied)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Elasticsearch sync dispatch cycle failed.")
            await asyncio.sleep(self._poll_interval)


__all__ = ["ElasticsearchSyncDispatcher"]
