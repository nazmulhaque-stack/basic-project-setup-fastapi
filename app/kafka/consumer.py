"""Abstract Base Consumer for Apache Kafka with manual commits, retries, and DLQ."""

from abc import ABC, abstractmethod
import asyncio
import logging
from typing import Any

from aiokafka import AIOKafkaConsumer
import orjson

from app.core.config import Settings, get_settings
from app.kafka.dlq import DeadLetterQueueService
from app.kafka.producer import KafkaProducerService
from app.kafka.retry import RetryPolicy

logger = logging.getLogger("fastapi_ha.kafka.consumer")


class BaseConsumer(ABC):
    """Abstract Base Consumer for Apache Kafka.

    Guarantees:
    1. Zero Data Loss: Commits Kafka offset only after successful processing.
    2. Resilient Error Handling: Exponential backoff retries with routing to DLQ on exhaustion.
    """

    topic: str = ""
    group_id: str = ""

    def __init__(
        self,
        producer: KafkaProducerService,
        settings: Settings | None = None,
    ) -> None:
        self.settings = settings or get_settings()
        self.producer = producer
        self.dlq = DeadLetterQueueService(producer=producer, settings=self.settings)
        self.retry_policy = RetryPolicy(
            max_retries=3,
            base_backoff_seconds=2.0,
        )
        self._consumer: AIOKafkaConsumer | None = None
        self._running = False

    @abstractmethod
    async def process_event(self, event_id: str, payload: dict[str, Any], raw_message: Any) -> None:
        """Business logic implementation."""
        pass

    async def start(self) -> None:
        if not self.settings.KAFKA_ENABLED:
            logger.info("Kafka disabled; consumer for topic '%s' not started.", self.topic)
            return

        logger.info(
            "Starting consumer for topic '%s' [group: %s] at %s",
            self.topic,
            self.group_id,
            self.settings.KAFKA_BOOTSTRAP_SERVERS,
        )

        self._consumer = AIOKafkaConsumer(
            self.topic,
            bootstrap_servers=self.settings.KAFKA_BOOTSTRAP_SERVERS.split(","),
            group_id=self.group_id,
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            value_deserializer=lambda v: orjson.loads(v) if v else {},
        )
        await self._consumer.start()
        self._running = True

        try:
            while self._running:
                msg_dict = await self._consumer.getmany(timeout_ms=1000, max_records=20)
                if not msg_dict:
                    continue

                for tp, messages in msg_dict.items():
                    for msg in messages:
                        await self._handle_single_message(msg)

        except asyncio.CancelledError:
            logger.info("Consumer loop cancelled for topic '%s'", self.topic)
        finally:
            await self.stop()

    async def _handle_single_message(self, msg: Any) -> None:
        data = msg.value if isinstance(msg.value, dict) else {}
        event_id = data.get("event_id") or f"gen-{msg.partition}-{msg.offset}"
        payload = data.get("payload", data)
        msg_key = msg.key.decode("utf-8") if msg.key else None

        attempt = 1
        while attempt <= self.retry_policy.max_retries:
            try:
                await self.process_event(event_id, payload, msg)
                await self._consumer.commit()
                return

            except Exception as exc:
                if not self.retry_policy.is_retryable(exc):
                    logger.error(
                        "Non-retryable error processing event %s: %s. Sending to DLQ.",
                        event_id,
                        exc,
                    )
                    await self.dlq.send_to_dlq(self.topic, msg_key, data, exc, retry_count=attempt)
                    await self._consumer.commit()
                    return

                if attempt >= self.retry_policy.max_retries:
                    logger.critical(
                        "Max retries (%d) exhausted for event %s. Escalating to DLQ.",
                        self.retry_policy.max_retries,
                        event_id,
                    )
                    await self.dlq.send_to_dlq(self.topic, msg_key, data, exc, retry_count=attempt)
                    await self._consumer.commit()
                    return

                delay = self.retry_policy.calculate_backoff(attempt)
                logger.warning(
                    "Error processing event %s (attempt %d/%d). Retrying in %.2fs. Error: %s",
                    event_id,
                    attempt,
                    self.retry_policy.max_retries,
                    delay,
                    exc,
                )
                await asyncio.sleep(delay)
                attempt += 1

    async def stop(self) -> None:
        self._running = False
        if self._consumer:
            logger.info("Stopping Kafka Consumer for topic '%s'...", self.topic)
            await self._consumer.stop()
            self._consumer = None
            logger.info("Kafka Consumer stopped.")


__all__ = ["BaseConsumer"]
