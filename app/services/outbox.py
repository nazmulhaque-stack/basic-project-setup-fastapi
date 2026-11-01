"""Background dispatcher for transactional outbox events."""

import asyncio
from datetime import datetime, timezone
import logging

from sqlalchemy import and_, exists, or_, select
from sqlalchemy.ext.asyncio import async_sessionmaker, AsyncSession
from sqlalchemy.orm import aliased

from app.core.kafka import KafkaEventProducer
from app.models.outbox import OutboxEvent

logger = logging.getLogger(__name__)


class OutboxDispatcher:
    """Publishes committed outbox rows to Kafka with at-least-once delivery."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        producer: KafkaEventProducer,
        *,
        poll_interval: float = 1.0,
        batch_size: int = 100,
    ) -> None:
        self._session_factory = session_factory
        self._producer = producer
        self._poll_interval = poll_interval
        self._batch_size = batch_size
        self._task: asyncio.Task[None] | None = None

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
        async with self._session_factory() as session:
            async with session.begin():
                earlier_event = aliased(OutboxEvent)
                has_earlier_unpublished_event = exists(
                    select(1).where(
                        earlier_event.topic == OutboxEvent.topic,
                        earlier_event.event_key == OutboxEvent.event_key,
                        earlier_event.published_at.is_(None),
                        or_(
                            earlier_event.created_at < OutboxEvent.created_at,
                            and_(
                                earlier_event.created_at == OutboxEvent.created_at,
                                earlier_event.id < OutboxEvent.id,
                            ),
                        ),
                    )
                )
                result = await session.execute(
                    select(OutboxEvent)
                    .where(OutboxEvent.published_at.is_(None))
                    .where(~has_earlier_unpublished_event)
                    .order_by(OutboxEvent.created_at, OutboxEvent.id)
                    .limit(self._batch_size)
                    .with_for_update(skip_locked=True)
                )
                events = result.scalars().all()
                for event in events:
                    try:
                        await self._producer.publish(
                            event.topic,
                            event.event_key,
                            event.payload,
                        )
                    except Exception as exc:
                        event.attempts += 1
                        event.last_error = str(exc)[:4000]
                        logger.exception(
                            "Could not publish outbox event %s; it will be retried.",
                            event.id,
                        )
                        continue

                    event.published_at = datetime.now(timezone.utc)
                    event.last_error = None
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
