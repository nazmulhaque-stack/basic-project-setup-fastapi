"""Tests for transactional domain events and Kafka outbox delivery.

Integration tests here run against a real Postgres connection via the `pg_pool`
fixture (see `tests/conftest.py`) and self-clean their rows, since there is no
in-memory substitute that faithfully reproduces asyncpg/Postgres behavior now
that the project talks to the database only via raw asyncpg (no SQLAlchemy,
no SQLite fallback).
"""

from datetime import datetime, timezone
from typing import Any
import uuid
from uuid import uuid4

import pytest

from app.db.pool import UnitOfWork
from app.schemas.category import CategoryCreate
from app.services.category import CategoryService
from app.services.outbox import OutboxDispatcher


class FakeProducer:
    def __init__(self) -> None:
        self.events: list[tuple[str, str, dict[str, object]]] = []

    async def publish(self, topic: str, key: str, event: dict[str, object]) -> None:
        self.events.append((topic, key, event))


@pytest.mark.asyncio
async def test_domain_mutation_and_event_share_transaction(pg_pool: Any) -> None:
    unique = uuid.uuid4().hex[:8]
    category_ids: list[int] = []

    try:
        async with pg_pool.acquire() as connection:
            # 1. Default: auto_outbox_events is False -> generic create does NOT trigger outbox event
            async with UnitOfWork(connection) as uow:
                service = CategoryService(db=uow, outbox_enabled=True, outbox_topic="domain.events")
                cat_default = await service.create(CategoryCreate(name=f"Clothing-{unique}"))
                category_ids.append(cat_default.id)
                no_event = await connection.fetchval(
                    "SELECT count(*) FROM outbox_events WHERE event_key = $1", str(cat_default.id)
                )
                assert no_event == 0

                # 2. Developer explicitly requests event triggering on create via emit_outbox_event=True
                category = await service.create(
                    CategoryCreate(name=f"Electronics-{unique}"),
                    emit_outbox_event=True,
                )
                category_ids.append(category.id)
                event_row = await connection.fetchrow(
                    "SELECT * FROM outbox_events WHERE event_key = $1", str(category.id)
                )
                assert event_row is not None
                assert event_row["topic"] == "domain.events"
                assert event_row["event_key"] == str(category.id)
                assert event_row["event_type"] == "HighPerfStatelessService.category.created"
                assert event_row["payload"]["event_type"] == "HighPerfStatelessService.category.created"
                assert event_row["payload"]["aggregate_id"] == str(category.id)
                assert event_row["payload"]["data"] == {"id": str(category.id)}
                assert event_row["published_at"] is None

                # 3. Developer explicitly enqueues manual event via enqueue_outbox_event
                manual_event = await service.enqueue_outbox_event(
                    "created",
                    cat_default,
                    version="v1",
                )
                assert manual_event is not None
                assert manual_event.event_type == "HighPerfStatelessService.category.created.v1"

            # 4. Outbox disabled flag prevents all outbox events
            async with UnitOfWork(connection) as uow:
                disabled_service = CategoryService(db=uow, outbox_enabled=False)
                book = await disabled_service.create(
                    CategoryCreate(name=f"Books-{unique}"), emit_outbox_event=True
                )
                category_ids.append(book.id)
                manual = await disabled_service.enqueue_outbox_event("created", cat_default)
                assert manual is None
                count = await connection.fetchval(
                    "SELECT count(*) FROM outbox_events WHERE event_key = $1", str(book.id)
                )
                assert count == 0
    finally:
        if category_ids:
            async with pg_pool.acquire() as connection:
                await connection.execute(
                    "DELETE FROM outbox_events WHERE event_key = ANY($1::text[])",
                    [str(c) for c in category_ids],
                )
                await connection.execute(
                    "DELETE FROM categories WHERE id = ANY($1::int[])", category_ids
                )


@pytest.mark.asyncio
async def test_outbox_dispatcher_publishes_and_marks_event(pg_pool: Any) -> None:
    event_id = uuid4()
    try:
        async with pg_pool.acquire() as connection:
            await connection.execute(
                """
                INSERT INTO outbox_events (id, topic, event_key, event_type, payload, created_at)
                VALUES ($1, $2, $3, $4, $5, $6)
                """,
                event_id,
                "domain.events",
                "category-1",
                "Category.created.v1",
                {"event_id": str(event_id), "aggregate_id": "category-1"},
                datetime.now(timezone.utc),
            )

        producer = FakeProducer()
        dispatcher = OutboxDispatcher(pg_pool, producer, batch_size=10)
        assert await dispatcher.dispatch_once() == 1
        assert producer.events[0][0:2] == ("domain.events", "category-1")

        async with pg_pool.acquire() as connection:
            row = await connection.fetchrow("SELECT * FROM outbox_events WHERE id = $1", event_id)
            assert row is not None
            assert row["published_at"] is not None
            assert row["last_error"] is None
    finally:
        async with pg_pool.acquire() as connection:
            await connection.execute("DELETE FROM outbox_events WHERE id = $1", event_id)


@pytest.mark.asyncio
async def test_outbox_dispatcher_preserves_order_for_each_aggregate(pg_pool: Any) -> None:
    first_id = uuid4()
    second_id = uuid4()
    try:
        async with pg_pool.acquire() as connection:
            await connection.execute(
                """
                INSERT INTO outbox_events (id, topic, event_key, event_type, payload, created_at)
                VALUES ($1, $2, $3, $4, $5, $6)
                """,
                first_id,
                "domain.events",
                "category-1",
                "category.updated.v1",
                {"event_id": str(first_id)},
                datetime(2026, 1, 1, tzinfo=timezone.utc),
            )
            await connection.execute(
                """
                INSERT INTO outbox_events (id, topic, event_key, event_type, payload, created_at)
                VALUES ($1, $2, $3, $4, $5, $6)
                """,
                second_id,
                "domain.events",
                "category-1",
                "category.deleted.v1",
                {"event_id": str(second_id)},
                datetime(2026, 1, 2, tzinfo=timezone.utc),
            )

        producer = FakeProducer()
        dispatcher = OutboxDispatcher(pg_pool, producer, batch_size=10)
        assert await dispatcher.dispatch_once() == 1
        assert [event[2]["event_id"] for event in producer.events] == [str(first_id)]

        assert await dispatcher.dispatch_once() == 1
        assert [event[2]["event_id"] for event in producer.events] == [
            str(first_id),
            str(second_id),
        ]
    finally:
        async with pg_pool.acquire() as connection:
            await connection.execute(
                "DELETE FROM outbox_events WHERE id = ANY($1::uuid[])", [first_id, second_id]
            )


def test_format_event_type_convention() -> None:
    from app.services.base import format_event_type

    # Format: <service_name>.<model_name>.<action_name>[.<version>]
    # 1. Default service prefix from APP_NAME (HighPerfStatelessService) without version
    assert format_event_type("user", "created") == "HighPerfStatelessService.user.created"
    assert format_event_type("User", "CREATED") == "HighPerfStatelessService.user.created"
    assert format_event_type("Category", "created") == "HighPerfStatelessService.category.created"
    assert format_event_type(model="user", action="created") == "HighPerfStatelessService.user.created"

    # 2. When version is provided:
    assert format_event_type("user", "created", "v1") == "HighPerfStatelessService.user.created.v1"
    assert format_event_type("User", "CREATED", "1") == "HighPerfStatelessService.user.created.v1"
    assert (
        format_event_type(" product ", " updated ", "v2")
        == "HighPerfStatelessService.product.updated.v2"
    )
    assert (
        format_event_type("inventory", "reservation_failed", "v1")
        == "HighPerfStatelessService.inventory.reservation_failed.v1"
    )
    assert (
        format_event_type("user", "created", version="v1")
        == "HighPerfStatelessService.user.created.v1"
    )

    # 3. Custom service name override:
    assert (
        format_event_type("AuthService", "user", "created")
        == "AuthService.user.created"
    )
    assert (
        format_event_type("AuthService", "user", "created", "v1")
        == "AuthService.user.created.v1"
    )
    assert (
        format_event_type(service="BillingService", model="invoice", action="paid")
        == "BillingService.invoice.paid"
    )
