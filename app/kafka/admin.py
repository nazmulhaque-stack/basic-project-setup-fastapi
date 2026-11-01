"""Administrative manager for Kafka topic provisioning and cluster inspection."""

import logging
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from app.core.config import Settings, get_settings

logger = logging.getLogger("fastapi_ha.kafka.admin")


class KafkaAdminService:
    """Administrative manager for Kafka topic provisioning and cluster inspection."""

    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or get_settings()

    async def _get_client(self) -> AIOKafkaAdminClient:
        client = AIOKafkaAdminClient(
            bootstrap_servers=self.settings.KAFKA_BOOTSTRAP_SERVERS.split(","),
            client_id="fastapi-admin-client",
        )
        await client.start()
        return client

    async def list_topics(self) -> list[str]:
        client = await self._get_client()
        try:
            topics = await client.list_topics()
            return sorted(list(topics))
        finally:
            await client.close()

    async def create_topic(
        self,
        name: str,
        num_partitions: int = 3,
        replication_factor: int = 1,
    ) -> bool:
        client = await self._get_client()
        try:
            existing = await client.list_topics()
            if name in existing:
                logger.info("Topic '%s' already exists.", name)
                return False

            new_topic = NewTopic(
                name=name,
                num_partitions=num_partitions,
                replication_factor=replication_factor,
            )
            await client.create_topics([new_topic])
            logger.info("Created topic '%s' (partitions=%d)", name, num_partitions)
            return True
        finally:
            await client.close()

    async def describe_topic(self, name: str) -> dict:
        client = await self._get_client()
        try:
            metadata = await client.describe_topics([name])
            return metadata[0] if metadata else {}
        finally:
            await client.close()


__all__ = ["KafkaAdminService"]
