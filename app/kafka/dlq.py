"""Manages Dead-Letter Queue (DLQ) publishing and message replay."""

from datetime import datetime, timezone
import logging
from typing import Any

from aiokafka import AIOKafkaConsumer

from app.core.config import Settings, get_settings
from app.kafka.producer import KafkaProducerService

logger = logging.getLogger("fastapi_ha.kafka.dlq")


class DeadLetterQueueService:
    """Manages Dead-Letter Queue (DLQ) publishing and message replay."""

    def __init__(
        self,
        producer: KafkaProducerService,
        settings: Settings | None = None,
    ) -> None:
        self.producer = producer
        self.settings = settings or get_settings()

    @staticmethod
    def get_dlq_topic(original_topic: str) -> str:
        return f"{original_topic}.DLQ"

    async def send_to_dlq(
        self,
        original_topic: str,
        message_key: str | None,
        payload: Any,
        exception: Exception,
        retry_count: int,
    ) -> None:
        dlq_topic = self.get_dlq_topic(original_topic)
        failed_at = datetime.now(timezone.utc).isoformat()

        headers = [
            ("x-original-topic", original_topic.encode("utf-8")),
            ("x-failed-at", failed_at.encode("utf-8")),
            ("x-error-type", type(exception).__name__.encode("utf-8")),
            ("x-error-message", str(exception).encode("utf-8")),
            ("x-retry-count", str(retry_count).encode("utf-8")),
        ]

        logger.error(
            "Routing poison message from topic '%s' to DLQ '%s' (Error: %s: %s)",
            original_topic,
            dlq_topic,
            type(exception).__name__,
            exception,
        )

        await self.producer.publish(
            topic=dlq_topic,
            key=message_key,
            event=payload,
            headers=headers,
        )

    async def replay_dlq(self, dlq_topic: str, max_messages: int = 100) -> int:
        consumer = AIOKafkaConsumer(
            dlq_topic,
            bootstrap_servers=self.settings.KAFKA_BOOTSTRAP_SERVERS.split(","),
            group_id=f"dlq-replay-{int(datetime.now().timestamp())}",
            auto_offset_reset="earliest",
            enable_auto_commit=True,
        )
        await consumer.start()
        replayed = 0

        try:
            logger.info("Starting DLQ replay for topic '%s' (limit=%d)...", dlq_topic, max_messages)
            while replayed < max_messages:
                msg_dict = await consumer.getmany(timeout_ms=3000, max_records=max_messages - replayed)
                if not msg_dict:
                    break

                for tp, messages in msg_dict.items():
                    for msg in messages:
                        orig_topic = None
                        if msg.headers:
                            for k, v in msg.headers:
                                if k == "x-original-topic":
                                    orig_topic = v.decode("utf-8")
                                    break

                        if not orig_topic:
                            orig_topic = dlq_topic.replace(".DLQ", "")

                        key_str = msg.key.decode("utf-8") if msg.key else None
                        await self.producer.publish(
                            topic=orig_topic,
                            key=key_str,
                            event=msg.value,
                        )
                        replayed += 1

            logger.info("DLQ replay completed: %d messages republished.", replayed)
            return replayed
        finally:
            await consumer.stop()


__all__ = ["DeadLetterQueueService"]
