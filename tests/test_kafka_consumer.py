"""Tests for safe per-record Kafka offset commits."""

import asyncio

import orjson
import pytest
from aiokafka.structs import OffsetAndMetadata, TopicPartition

from app.core import kafka


class FakeMessage:
    topic = "inventory.events"
    partition = 0

    def __init__(self, offset: int, value: bytes) -> None:
        self.offset = offset
        self.value = value


class FakeConsumer:
    def __init__(self, **_: object) -> None:
        self.partition = TopicPartition("inventory.events", 0)
        self.commits: list[dict[TopicPartition, OffsetAndMetadata]] = []
        self.seeks: list[tuple[TopicPartition, int]] = []
        self.polls = 0

    async def start(self) -> None:
        pass

    async def getmany(self, **_: object) -> dict[TopicPartition, list[FakeMessage]]:
        self.polls += 1
        if self.polls == 1:
            return {
                self.partition: [
                    FakeMessage(10, orjson.dumps({"id": "ok"})),
                    FakeMessage(11, orjson.dumps({"id": "retry"})),
                    FakeMessage(12, orjson.dumps({"id": "must-not-skip"})),
                ]
            }
        return {}

    async def commit(
        self,
        offsets: dict[TopicPartition, OffsetAndMetadata],
    ) -> None:
        self.commits.append(offsets)

    def seek(self, partition: TopicPartition, offset: int) -> None:
        self.seeks.append((partition, offset))

    async def stop(self) -> None:
        pass


@pytest.mark.asyncio
async def test_consumer_does_not_commit_past_a_failed_record(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    consumer = FakeConsumer()
    monkeypatch.setattr(kafka, "AIOKafkaConsumer", lambda *args, **kwargs: consumer)
    stop_event = asyncio.Event()
    handled: list[str] = []

    async def handler(event: dict[str, object]) -> None:
        event_id = str(event["id"])
        handled.append(event_id)
        if event_id == "retry":
            stop_event.set()
            raise RuntimeError("temporary handler failure")

    await kafka.consume_events(
        bootstrap_servers="localhost:9092",
        topics=["inventory.events"],
        group_id="inventory-test",
        handler=handler,
        stop_event=stop_event,
    )

    partition = TopicPartition("inventory.events", 0)
    assert handled == ["ok", "retry"]
    assert consumer.commits == [{partition: OffsetAndMetadata(11, "")}]
    assert consumer.seeks == [(partition, 11)]
