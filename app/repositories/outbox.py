"""Outbox repository: Hand-written asyncpg SQL queries for Transactional Outbox.

Enforces atomicity by saving outbox events inside the active UnitOfWork transaction,
and provides cluster-safe batch polling using `FOR UPDATE SKIP LOCKED`.
"""

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
import uuid

import orjson

from app.db.pool import UnitOfWork
from app.models.outbox import OutboxEvent


def _row_to_outbox(row: Any) -> OutboxEvent:
    raw_payload = row["payload"]
    if isinstance(raw_payload, str):
        payload_dict = orjson.loads(raw_payload)
    else:
        payload_dict = raw_payload or {}

    return OutboxEvent(
        id=row["id"],
        aggregate_type=row["aggregate_type"],
        aggregate_id=row["aggregate_id"],
        event_type=row["event_type"],
        payload=payload_dict,
        status=row["status"],
        retry_count=row["retry_count"],
        last_error=row.get("last_error"),
        created_at=row["created_at"],
        published_at=row.get("published_at"),
    )


class OutboxRepository:
    """Asyncpg repository for Transactional Outbox events."""

    def __init__(self, db: UnitOfWork) -> None:
        self.db = db

    async def create_event(
        self,
        aggregate_type: str,
        aggregate_id: str,
        event_type: str,
        payload: Dict[str, Any],
        event_id: Optional[uuid.UUID] = None,
    ) -> OutboxEvent:
        """Inserts an outbox event within the active transaction."""
        event_uuid = event_id or uuid.uuid4()
        now = datetime.now(timezone.utc)
        payload_json = orjson.dumps(payload).decode("utf-8")

        query = """
            INSERT INTO outbox (id, aggregate_type, aggregate_id, event_type, payload, status, retry_count, created_at)
            VALUES ($1, $2, $3, $4, $5::jsonb, 'PENDING', 0, $6)
            RETURNING *
        """
        row = await self.db.fetchrow(
            query,
            event_uuid,
            aggregate_type,
            aggregate_id,
            event_type,
            payload_json,
            now,
        )
        return _row_to_outbox(row)

    async def get(self, event_id: uuid.UUID) -> Optional[OutboxEvent]:
        """Fetch single outbox event by ID."""
        row = await self.db.fetchrow("SELECT * FROM outbox WHERE id = $1", event_id)
        return _row_to_outbox(row) if row else None

    async def fetch_pending_batch(self, limit: int = 50) -> List[OutboxEvent]:
        """Fetches pending outbox events using FOR UPDATE SKIP LOCKED for concurrent worker safety."""
        query = """
            SELECT *
            FROM outbox
            WHERE status = 'PENDING'
            ORDER BY created_at ASC
            LIMIT $1
            FOR UPDATE SKIP LOCKED
        """
        rows = await self.db.fetch(query, limit)
        return [_row_to_outbox(r) for r in rows]

    async def mark_published(self, event_id: uuid.UUID, published_at: datetime) -> bool:
        """Marks event as PUBLISHED with exact timestamp when Elasticsearch indexing succeeded."""
        query = """
            UPDATE outbox
            SET status = 'PUBLISHED', published_at = $2, last_error = NULL
            WHERE id = $1
        """
        res = await self.db.execute(query, event_id, published_at)
        return res != "UPDATE 0"

    async def mark_failed(
        self,
        event_id: uuid.UUID,
        error_message: str,
        max_retries: int = 5,
    ) -> bool:
        """Increments retry count and sets FAILED status if maximum retry count is reached."""
        query = """
            UPDATE outbox
            SET retry_count = retry_count + 1,
                last_error = $2,
                status = CASE WHEN retry_count + 1 >= $3 THEN 'FAILED' ELSE 'PENDING' END
            WHERE id = $1
        """
        res = await self.db.execute(query, event_id, error_message, max_retries)
        return res != "UPDATE 0"

    async def list_events(
        self,
        limit: int = 50,
        status: Optional[str] = None,
    ) -> List[OutboxEvent]:
        """Lists recent outbox events for monitoring and audit inspection."""
        if status:
            query = "SELECT * FROM outbox WHERE status = $1 ORDER BY created_at DESC LIMIT $2"
            rows = await self.db.fetch(query, status, limit)
        else:
            query = "SELECT * FROM outbox ORDER BY created_at DESC LIMIT $1"
            rows = await self.db.fetch(query, limit)
        return [_row_to_outbox(r) for r in rows]


__all__ = ["OutboxRepository"]
