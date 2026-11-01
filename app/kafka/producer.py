"""Asynchronous Kafka Producer service for event streaming."""

import logging
from typing import Any

from aiokafka import AIOKafkaProducer
import orjson
from pydantic import BaseModel

from app.core.config import Settings, get_settings
from app.kafka.events.base import BaseEvent

logger = logging.getLogger("fastapi_ha.kafka.producer")


def _json_serializer(value: Any) -> bytes:
    if isinstance(value, BaseModel):
        return orjson.dumps(value.model_dump(mode="json"))
    elif isinstance(value, (dict, list, str, int, float, bool)):
        return orjson.dumps(value)
    elif isinstance(value, bytes):
        return value
    return orjson.dumps(str(value))


class KafkaProducerService:
    """Production-grade asynchronous Kafka Producer using aiokafka."""

    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or get_settings()
        self._producer: AIOKafkaProducer | None = None
        self._started = False

    async def start(self) -> None:
        if not self.settings.KAFKA_ENABLED or self._started:
            return

        logger.info(
            "Starting Kafka Producer connecting to: %s",
            self.settings.KAFKA_BOOTSTRAP_SERVERS,
        )
        self._producer = AIOKafkaProducer(
            bootstrap_servers=self.settings.KAFKA_BOOTSTRAP_SERVERS.split(","),
            acks="all",
            value_serializer=_json_serializer,
            key_serializer=lambda k: k.encode("utf-8") if isinstance(k, str) else k,
            compression_type="gzip",
        )
        await self._producer.start()
        self._started = True
        logger.info("Kafka Producer started successfully.")

    async def stop(self) -> None:
        if self._producer and self._started:
            logger.info("Stopping Kafka Producer...")
            await self._producer.stop()
            self._started = False
            logger.info("Kafka Producer stopped.")

    async def publish(
        self,
        topic: str,
        event: BaseEvent | dict | BaseModel,
        key: str | None = None,
        headers: list[tuple[str, bytes]] | None = None,
    ) -> None:
        if not self.settings.KAFKA_ENABLED:
            logger.debug("Kafka disabled; skipping publish to topic '%s'", topic)
            return

        if not self._started or self._producer is None:
            raise RuntimeError("KafkaProducerService has not been started. Call 'await start()' first.")

        try:
            record_metadata = await self._producer.send_and_wait(
                topic=topic,
                value=event,
                key=key,
                headers=headers,
            )
            logger.debug(
                "Published event to %s [partition=%d, offset=%d]",
                topic,
                record_metadata.partition,
                record_metadata.offset,
            )
        except Exception as exc:
            logger.error("Failed to publish event to topic '%s': %s", topic, exc)
            raise


__all__ = ["KafkaProducerService"]
