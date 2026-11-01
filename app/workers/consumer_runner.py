"""Standalone worker supervisor for Kafka consumers, outbox dispatcher, and background sweepers."""

import asyncio
import logging
from typing import Any

from app.core.config import Settings, get_settings
from app.core.kafka import KafkaEventProducer, consume_events_forever
from app.db.session import create_db_engine, create_session_factory
from app.services.inventory_reservations import (
    handle_inventory_reserved,
    run_expiration_sweeper,
)
from app.services.outbox import OutboxDispatcher

logger = logging.getLogger("fastapi_ha.workers.runner")


class WorkerSupervisor:
    """Supervisor running background consumers and the transactional outbox dispatcher.

    Allows running consumers in a dedicated worker process or container,
    completely separate from HTTP API workers.
    """

    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or get_settings()
        self.engine = create_db_engine(self.settings)
        self.session_factory = create_session_factory(self.engine)
        self._stop_event = asyncio.Event()
        self._tasks: list[asyncio.Task] = []
        self._producer: KafkaEventProducer | None = None
        self._outbox_dispatcher: OutboxDispatcher | None = None

    async def run(self) -> None:
        logger.info("Starting WorkerSupervisor daemon...")

        if not self.settings.KAFKA_ENABLED:
            logger.warning("KAFKA_ENABLED=false. Worker will idle.")
            await self._stop_event.wait()
            return

        # 1. Start Kafka Producer
        self._producer = KafkaEventProducer(self.settings.KAFKA_BOOTSTRAP_SERVERS)
        await self._producer.start()

        # 2. Start Outbox Dispatcher
        self._outbox_dispatcher = OutboxDispatcher(
            self.session_factory,
            self._producer,
            poll_interval=self.settings.OUTBOX_POLL_INTERVAL,
            batch_size=self.settings.OUTBOX_BATCH_SIZE,
        )
        self._outbox_dispatcher.start()

        # 3. Inventory Consumer handler
        async def handle_inventory_event(event: dict[str, Any]) -> None:
            await handle_inventory_reserved(
                event,
                self.session_factory,
                self._producer,
                output_topic=self.settings.KAFKA_EVENTS_TOPIC,
                dead_letter_topic=self.settings.KAFKA_INVENTORY_DLQ_TOPIC,
            )

        # 4. Spawn consumer and sweeper tasks
        consumer_task = asyncio.create_task(
            consume_events_forever(
                bootstrap_servers=self.settings.KAFKA_BOOTSTRAP_SERVERS,
                topics=[self.settings.KAFKA_INVENTORY_TOPIC],
                group_id=self.settings.KAFKA_INVENTORY_CONSUMER_GROUP,
                handler=handle_inventory_event,
                stop_event=self._stop_event,
            ),
            name="worker-kafka-inventory-consumer",
        )
        self._tasks.append(consumer_task)

        sweeper_task = asyncio.create_task(
            run_expiration_sweeper(
                self.session_factory,
                self.settings.KAFKA_EVENTS_TOPIC,
                interval=self.settings.INVENTORY_RESERVATION_SWEEP_INTERVAL,
                stop_event=self._stop_event,
            ),
            name="worker-inventory-expiration-sweeper",
        )
        self._tasks.append(sweeper_task)

        logger.info("WorkerSupervisor initialized with %d background tasks.", len(self._tasks))
        await self._stop_event.wait()

        # Graceful shutdown
        logger.info("Shutting down WorkerSupervisor tasks...")
        if self._outbox_dispatcher:
            await self._outbox_dispatcher.stop()

        for t in self._tasks:
            t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)

        if self._producer:
            await self._producer.stop()

        await self.engine.dispose()
        logger.info("WorkerSupervisor shutdown complete.")

    def stop(self) -> None:
        self._stop_event.set()


__all__ = ["WorkerSupervisor"]
