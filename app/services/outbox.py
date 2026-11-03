"""Background dispatcher for transactional outbox events (raw asyncpg, no ORM)."""

import asyncio
from datetime import datetime, timezone
import logging
from typing import Optional

import asyncpg

from app.core.kafka import KafkaEventProducer

logger = logging.getLogger(__name__)

# Guarantees strict FIFO delivery per aggregate: a later event for the same
# (topic, event_key) is never dispatched while an earlier one is still
# unpublished. `SKIP LOCKED` lets multiple dispatcher processes poll
# concurrently without blocking on each other's in-flight batches.
_SELECT_PENDING_SQL = """
SELECT * FROM outbox_events AS o
WHERE o.published_at IS NULL
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


class OutboxDispatcher:
    """Publishes committed outbox rows to Kafka with at-least-once delivery."""

    def __init__(
        self,
        pool: asyncpg.Pool,
        producer: KafkaEventProducer,
        *,
        poll_interval: float = 1.0,
        batch_size: int = 100,
    ) -> None:
        self._pool = pool
        self._producer = producer
        self._poll_interval = poll_interval
        self._batch_size = batch_size
        self._task: Optional[asyncio.Task[None]] = None

    def start(self) -> None:
        if self._task is not None:
            raise RuntimeError("Outbox dispatcher is already running.")
        self._task = asyncio.create_task(self._run(), name="kafka-outbox-dispatcher")

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

    async def dispatch_once(self) -> int:
        """Publish one batch and mark successful rows in the same DB transaction."""
        published = 0
        async with self._pool.acquire() as connection:
            async with connection.transaction():
                rows = await connection.fetch(_SELECT_PENDING_SQL, self._batch_size)
                for row in rows:
                    try:
                        await self._producer.publish(
                            row["topic"],
                            row["event_key"],
                            row["payload"],
                        )
                    except Exception as exc:
                        await connection.execute(
                            "UPDATE outbox_events SET attempts = attempts + 1, last_error = $2 WHERE id = $1",
                            row["id"],
                            str(exc)[:4000],
                        )
                        logger.exception(
                            "Could not publish outbox event %s; it will be retried.",
                            row["id"],
                        )
                        continue

                    await connection.execute(
                        "UPDATE outbox_events SET published_at = $2, last_error = NULL WHERE id = $1",
                        row["id"],
                        datetime.now(timezone.utc),
                    )
                    published += 1
        return published

    async def _run(self) -> None:
        while True:
            try:
                published = await self.dispatch_once()
                if published:
                    logger.info("Published %d Kafka outbox event(s).", published)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Kafka outbox dispatch cycle failed.")
            await asyncio.sleep(self._poll_interval)


__all__ = ["OutboxDispatcher"]
