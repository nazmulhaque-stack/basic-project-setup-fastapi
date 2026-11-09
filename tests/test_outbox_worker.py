"""Tests for Elasticsearch delivery result handling in the outbox worker."""

from datetime import datetime, timezone
import uuid

import pytest

from app.models.outbox import OutboxEvent
from app.services.outbox_worker import OutboxWorker


class FailedElasticsearch:
    async def index_document(self, index: str, id_: str, document: dict[str, object]) -> bool:
        return False

    async def update_document(self, index: str, id_: str, document: dict[str, object]) -> bool:
        return False

    async def delete_document(self, index: str, id_: str) -> bool:
        return False

    async def get_document(self, index: str, id_: str) -> None:
        return None

    async def search(self, index: str, query: str | dict[str, object], limit: int = 20, offset: int = 0):
        return [], 0

    async def search_raw(self, index: str, body: dict[str, object]) -> dict[str, object]:
        return {}

    async def ping(self) -> bool:
        return False

    async def close(self) -> None:
        return None


def make_event(event_type: str, payload: dict[str, object]) -> OutboxEvent:
    return OutboxEvent(
        id=uuid.uuid4(),
        aggregate_type="user",
        aggregate_id=str(uuid.uuid4()),
        event_type=event_type,
        payload=payload,
        created_at=datetime.now(timezone.utc),
    )


@pytest.mark.asyncio
async def test_worker_rejects_unsuccessful_user_indexing() -> None:
    worker = OutboxWorker(db_pool=None, elasticsearch=FailedElasticsearch())
    event = make_event("CREATE", {"user": {"id": "user-1"}, "profile": {"id": "user-1"}})

    with pytest.raises(RuntimeError, match="Elasticsearch rejected user document"):
        await worker._sync_single_event_to_es(event)


@pytest.mark.asyncio
async def test_worker_rejects_unsuccessful_user_deletion() -> None:
    worker = OutboxWorker(db_pool=None, elasticsearch=FailedElasticsearch())
    event = make_event("DELETE", {})

    with pytest.raises(RuntimeError, match="Elasticsearch rejected deletion"):
        await worker._sync_single_event_to_es(event)
