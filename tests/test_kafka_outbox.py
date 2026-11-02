"""Tests for transactional domain events and Kafka outbox delivery."""

from datetime import datetime, timezone
from uuid import uuid4

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.db.base import Base
from app.models.outbox import OutboxEvent
from app.schemas.category import CategoryCreate
from app.services.category import CategoryService
from app.services.outbox import OutboxDispatcher


class FakeProducer:
    def __init__(self) -> None:
        self.events: list[tuple[str, str, dict[str, object]]] = []

    async def publish(self, topic: str, key: str, event: dict[str, object]) -> None:
        self.events.append((topic, key, event))


@pytest.mark.asyncio
async def test_domain_mutation_and_event_share_transaction() -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)

    async with session_factory() as session:
        # 1. Default: auto_outbox_events is False -> generic create does NOT trigger outbox event
        service = CategoryService(
            db=session,
            outbox_enabled=True,
            outbox_topic="domain.events",
        )
        cat_default = await service.create(CategoryCreate(name="Clothing"))
        res = await session.execute(select(OutboxEvent))
        assert res.scalar_one_or_none() is None

        # 2. Developer explicitly requests event triggering on create via emit_outbox_event=True
        category = await service.create(
            CategoryCreate(name="Electronics"),
            emit_outbox_event=True,
        )
        result = await session.execute(select(OutboxEvent))
        event = result.scalar_one()

        assert event.topic == "domain.events"
        assert event.event_key == str(category.id)
        assert event.event_type == "HighPerfStatelessService.category.created"
        assert event.payload["event_type"] == "HighPerfStatelessService.category.created"
        assert event.payload["aggregate_id"] == str(category.id)
        assert event.payload["data"] == {"id": str(category.id)}
        assert event.published_at is None

        # 3. Developer explicitly enqueues manual event via enqueue_outbox_event
        manual_event = service.enqueue_outbox_event(
            "created",
            cat_default,
            version="v1",
        )
        assert manual_event is not None
        assert manual_event.event_type == "HighPerfStatelessService.category.created.v1"

    # 4. Outbox disabled flag prevents all outbox events
    async with session_factory() as session:
        disabled_service = CategoryService(
            db=session,
            outbox_enabled=False,
        )
        await disabled_service.create(CategoryCreate(name="Books"), emit_outbox_event=True)
        manual = disabled_service.enqueue_outbox_event("created", cat_default)
        assert manual is None

    await engine.dispose()


@pytest.mark.asyncio
async def test_outbox_dispatcher_publishes_and_marks_event() -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as connection:
        await connection.run_sync(OutboxEvent.__table__.create)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    event_id = uuid4()
    async with session_factory() as session:
        session.add(
            OutboxEvent(
                id=event_id,
                topic="domain.events",
                event_key="category-1",
                event_type="Category.created.v1",
                payload={"event_id": str(event_id), "aggregate_id": "category-1"},
                created_at=datetime.now(timezone.utc),
            )
        )
        await session.commit()

    producer = FakeProducer()
    dispatcher = OutboxDispatcher(session_factory, producer, batch_size=10)
    assert await dispatcher.dispatch_once() == 1
    assert producer.events[0][0:2] == ("domain.events", "category-1")

    async with session_factory() as session:
        event = await session.get(OutboxEvent, event_id)
        assert event is not None
        assert event.published_at is not None
        assert event.last_error is None

    await engine.dispose()


@pytest.mark.asyncio
async def test_outbox_dispatcher_preserves_order_for_each_aggregate() -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as connection:
        await connection.run_sync(OutboxEvent.__table__.create)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    first_id = uuid4()
    second_id = uuid4()
    async with session_factory() as session:
        session.add_all(
            [
                OutboxEvent(
                    id=first_id,
                    topic="domain.events",
                    event_key="category-1",
                    event_type="category.updated.v1",
                    payload={"event_id": str(first_id)},
                    created_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
                ),
                OutboxEvent(
                    id=second_id,
                    topic="domain.events",
                    event_key="category-1",
                    event_type="category.deleted.v1",
                    payload={"event_id": str(second_id)},
                    created_at=datetime(2026, 1, 2, tzinfo=timezone.utc),
                ),
            ]
        )
        await session.commit()

    producer = FakeProducer()
    dispatcher = OutboxDispatcher(session_factory, producer, batch_size=10)
    assert await dispatcher.dispatch_once() == 1
    assert [event[2]["event_id"] for event in producer.events] == [str(first_id)]

    assert await dispatcher.dispatch_once() == 1
    assert [event[2]["event_id"] for event in producer.events] == [
        str(first_id),
        str(second_id),
    ]

    await engine.dispose()

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
