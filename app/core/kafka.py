"""Async Kafka producer and consumer helpers for service-to-service events."""

from collections.abc import Awaitable, Callable, Sequence
import asyncio
from datetime import datetime, timezone
import logging
from typing import Any, Optional

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
    producer: Optional["KafkaEventProducer"] = None,
    dlq_topic: Optional[str] = None,
    max_retries: int = 3,
    base_backoff_seconds: float = 2.0,
) -> None:
    """Consume events and commit offsets only after the handler succeeds.

    Delivery is at least once. Handlers should be idempotent and can use the
    envelope's `event_id` for deduplication, and should catch & dead-letter
    their own expected/validation failures (so they return normally rather
    than raising) when they can classify an event as permanently invalid.

    Any exception that still escapes the handler is retried in place up to
    `max_retries` times with exponential backoff (`base_backoff_seconds * 2**attempt`),
    without blocking other partitions' messages in the same batch. If every
    retry fails and both `producer` and `dlq_topic` are supplied, the message
    is dead-lettered (with `x-original-topic`/`x-original-partition`/
    `x-original-offset`/`x-error-message`/`x-retry-count`/`x-failed-at`
    metadata) and its offset is committed so the partition keeps moving. If no
    DLQ is configured, the message is retried indefinitely instead of being
    silently dropped — configure `producer`/`dlq_topic` for any consumer where
    an unrecoverable error should not permanently stall the partition.
    """
    consumer = AIOKafkaConsumer(
        *topics,
        bootstrap_servers=bootstrap_servers.split(","),
        group_id=group_id,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
    )
    await consumer.start()
    # Tracks in-progress retry attempts per (topic, partition, offset); cleared on
    # success or once a message is dead-lettered.
    retry_counts: dict[tuple[str, int, int], int] = {}
    try:
        while not stop_event.is_set():
            records = await consumer.getmany(timeout_ms=1000, max_records=100)
            for topic_partition, messages in records.items():
                for message in messages:
                    retry_key = (message.topic, message.partition, message.offset)
                    try:
                        event = orjson.loads(message.value)
                        if not isinstance(event, dict):
                            raise ValueError("Kafka event envelope must be a JSON object.")
                        await handler(event)
                    except asyncio.CancelledError:
                        raise
                    except Exception as exc:
                        attempt = retry_counts.get(retry_key, 0) + 1
                        retry_counts[retry_key] = attempt
                        backoff = base_backoff_seconds * (2 ** (attempt - 1))

                        if attempt <= max_retries:
                            logger.warning(
                                "Kafka handler failed for %s partition %d offset %d "
                                "(attempt %d/%d); retrying in %.1fs: %s",
                                message.topic, message.partition, message.offset,
                                attempt, max_retries, backoff, exc,
                            )
                            consumer.seek(topic_partition, message.offset)
                            await asyncio.sleep(backoff)
                            break

                        retry_counts.pop(retry_key, None)
                        if producer is not None and dlq_topic:
                            try:
                                await producer.publish(
                                    dlq_topic,
                                    key=(
                                        message.key.decode("utf-8", errors="replace")
                                        if message.key
                                        else f"{message.topic}-{message.partition}-{message.offset}"
                                    ),
                                    event={
                                        "x-original-topic": message.topic,
                                        "x-original-partition": message.partition,
                                        "x-original-offset": message.offset,
                                        "x-error-message": str(exc),
                                        "x-retry-count": attempt,
                                        "x-failed-at": datetime.now(timezone.utc).isoformat(),
                                        "raw_value": message.value.decode("utf-8", errors="replace"),
                                    },
                                )
                                logger.error(
                                    "Kafka handler exhausted %d retries for %s partition %d "
                                    "offset %d; dead-lettered to %s: %s",
                                    max_retries, message.topic, message.partition,
                                    message.offset, dlq_topic, exc,
                                )
                            except asyncio.CancelledError:
                                raise
                            except Exception:
                                logger.exception(
                                    "Failed to dead-letter %s partition %d offset %d; "
                                    "will retry original message instead of dropping it.",
                                    message.topic, message.partition, message.offset,
                                )
                                consumer.seek(topic_partition, message.offset)
                                await asyncio.sleep(backoff)
                                break
                        else:
                            logger.error(
                                "Kafka handler exhausted %d retries for %s partition %d "
                                "offset %d and no DLQ is configured; retrying indefinitely: %s",
                                max_retries, message.topic, message.partition, message.offset, exc,
                            )
                            consumer.seek(topic_partition, message.offset)
                            await asyncio.sleep(backoff)
                            break
                    else:
                        retry_counts.pop(retry_key, None)

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
    producer: Optional["KafkaEventProducer"] = None,
    dlq_topic: Optional[str] = None,
    max_retries: int = 3,
    base_backoff_seconds: float = 2.0,
) -> None:
    """Reconnect the consumer after broker-level failures until shutdown.

    `producer`/`dlq_topic` are forwarded to `consume_events()` so a handler
    exception that survives `max_retries` attempts is dead-lettered instead of
    retried forever on every broker reconnect.
    """
    while not stop_event.is_set():
        try:
            await consume_events(
                bootstrap_servers=bootstrap_servers,
                topics=topics,
                group_id=group_id,
                handler=handler,
                stop_event=stop_event,
                producer=producer,
                dlq_topic=dlq_topic,
                max_retries=max_retries,
                base_backoff_seconds=base_backoff_seconds,
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
