"""Async Kafka producer and consumer helpers for service-to-service events."""

from collections.abc import Awaitable, Callable, Sequence
import asyncio
import logging
from typing import Any

import orjson
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.structs import OffsetAndMetadata, TopicPartition

logger = logging.getLogger(__name__)

EventHandler = Callable[[dict[str, Any]], Awaitable[None]]


class KafkaEventProducer:
    """Thin asynchronous publisher for JSON event envelopes."""

    def __init__(self, bootstrap_servers: str) -> None:
        self._producer = AIOKafkaProducer(
            bootstrap_servers=bootstrap_servers.split(","),
            acks="all",
            enable_idempotence=True,
        )

    async def start(self) -> None:
        await self._producer.start()

    async def publish(
        self,
        topic: str,
        key: str,
        event: dict[str, Any],
    ) -> None:
        await self._producer.send_and_wait(
            topic,
            key=key.encode("utf-8"),
            value=orjson.dumps(event),
        )

    async def stop(self) -> None:
        await self._producer.stop()


async def consume_events(
    *,
    bootstrap_servers: str,
    topics: Sequence[str],
    group_id: str,
    handler: EventHandler,
    stop_event: asyncio.Event,
) -> None:
    """Consume events and commit offsets only after the handler succeeds.

    Delivery is at least once. Handlers should be idempotent and can use
    the envelope's `event_id` for deduplication.
    """
    consumer = AIOKafkaConsumer(
        *topics,
        bootstrap_servers=bootstrap_servers.split(","),
        group_id=group_id,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
    )
    await consumer.start()
    try:
        while not stop_event.is_set():
            records = await consumer.getmany(timeout_ms=1000, max_records=100)
            for topic_partition, messages in records.items():
                for message in messages:
                    try:
                        event = orjson.loads(message.value)
                        if not isinstance(event, dict):
                            raise ValueError("Kafka event envelope must be a JSON object.")
                        await handler(event)
                    except asyncio.CancelledError:
                        raise
                    except Exception:
                        logger.exception(
                            "Kafka handler failed for %s partition %d offset %d; "
                            "the offset will not be committed.",
                            message.topic,
                            message.partition,
                            message.offset,
                        )
                        consumer.seek(topic_partition, message.offset)
                        break

                    await consumer.commit(
                        {
                            topic_partition: OffsetAndMetadata(
                                message.offset + 1,
                                "",
                            )
                        }
                    )
    finally:
        await consumer.stop()


async def consume_events_forever(
    *,
    bootstrap_servers: str,
    topics: Sequence[str],
    group_id: str,
    handler: EventHandler,
    stop_event: asyncio.Event,
    retry_interval: float = 5.0,
) -> None:
    """Reconnect the consumer after broker-level failures until shutdown."""
    while not stop_event.is_set():
        try:
            await consume_events(
                bootstrap_servers=bootstrap_servers,
                topics=topics,
                group_id=group_id,
                handler=handler,
                stop_event=stop_event,
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Kafka consumer stopped unexpectedly; retrying.")
        if not stop_event.is_set():
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=retry_interval)
            except TimeoutError:
                pass


__all__ = [
    "KafkaEventProducer",
    "consume_events",
    "consume_events_forever",
    "EventHandler",
]
