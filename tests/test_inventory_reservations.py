"""Tests for reservation event idempotency, availability, and expiration."""

from datetime import datetime, timedelta, timezone
import uuid

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.db.base import Base
from app.models.inventory_reservation import InventoryReservation
from app.models.outbox import OutboxEvent
from app.models.product import Product
from app.services.inventory_reservations import (
    handle_inventory_reserved,
    release_expired_reservations_once,
)


class FakeProducer:
    def __init__(self) -> None:
        self.events: list[tuple[str, str, dict[str, object]]] = []

    async def publish(self, topic: str, key: str, event: dict[str, object]) -> None:
        self.events.append((topic, key, event))


def reservation_event(
    *,
    event_id: str = "evt-1",
    reservation_id: str = "res-1",
    product_id: str = "SKU-1",
    quantity: int = 2,
    ttl_minutes: int = 30,
) -> dict[str, object]:
    return {
        "event_id": event_id,
        "event_type": "inventory.reserved.v1",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "payload": {
            "reservation_id": reservation_id,
            "order_id": "order-1",
            "product_id": product_id,
            "quantity": quantity,
            "location_id": "warehouse-east",
            "ttl_minutes": ttl_minutes,
        },
    }


@pytest.fixture
async def session_factory() -> async_sessionmaker:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    yield factory
    await engine.dispose()


async def add_product(
    session_factory: async_sessionmaker,
    *,
    stock: int = 5,
) -> uuid.UUID:
    product_id = uuid.uuid4()
    async with session_factory() as session:
        session.add(
            Product(
                id=product_id,
                name="Widget",
                sku="SKU-1",
                price_cents=100,
                category="general",
                is_active=True,
                stock=stock,
                reserved_quantity=0,
            )
        )
        await session.commit()
    return product_id


@pytest.mark.asyncio
async def test_reservation_reduces_available_stock_but_preserves_on_hand(
    session_factory: async_sessionmaker,
) -> None:
    product_id = await add_product(session_factory)
    producer = FakeProducer()
    event = reservation_event()

    await handle_inventory_reserved(
        event,
        session_factory,
        producer,
        output_topic="domain.events",
        dead_letter_topic="inventory.events.dlq",
    )

    async with session_factory() as session:
        product = await session.get(Product, product_id)
        reservation = await session.scalar(select(InventoryReservation))
        assert product is not None
        assert product.stock == 5
        assert product.reserved_quantity == 2
        assert product.available_quantity == 3
        assert reservation is not None
        assert reservation.status == "reserved"
        assert reservation.location_id == "warehouse-east"
        assert reservation.expires_at is not None

    await handle_inventory_reserved(
        event,
        session_factory,
        producer,
        output_topic="domain.events",
        dead_letter_topic="inventory.events.dlq",
    )
    async with session_factory() as session:
        product = await session.get(Product, product_id)
        count = len((await session.scalars(select(InventoryReservation))).all())
        assert product is not None
        assert product.stock == 5
        assert product.reserved_quantity == 2
        assert count == 1


@pytest.mark.asyncio
async def test_insufficient_stock_records_rejection_and_compensation(
    session_factory: async_sessionmaker,
) -> None:
    product_id = await add_product(session_factory, stock=1)
    producer = FakeProducer()
    event = reservation_event(quantity=2)

    await handle_inventory_reserved(
        event,
        session_factory,
        producer,
        output_topic="domain.events",
        dead_letter_topic="inventory.events.dlq",
    )
    await handle_inventory_reserved(
        event,
        session_factory,
        producer,
        output_topic="domain.events",
        dead_letter_topic="inventory.events.dlq",
    )

    async with session_factory() as session:
        product = await session.get(Product, product_id)
        reservation = await session.scalar(select(InventoryReservation))
        outbox_events = (
            await session.scalars(
                select(OutboxEvent).where(
                    OutboxEvent.event_type == "inventory.reservation_failed.v1"
                )
            )
        ).all()
        assert product is not None
        assert product.stock == 1
        assert product.reserved_quantity == 0
        assert reservation is not None
        assert reservation.status == "rejected"
        assert len(outbox_events) == 1


@pytest.mark.asyncio
async def test_unknown_product_is_dead_lettered(
    session_factory: async_sessionmaker,
) -> None:
    producer = FakeProducer()
    await handle_inventory_reserved(
        reservation_event(product_id="MISSING-SKU"),
        session_factory,
        producer,
        output_topic="domain.events",
        dead_letter_topic="inventory.events.dlq",
    )

    assert len(producer.events) == 1
    topic, _, dead_letter = producer.events[0]
    assert topic == "inventory.events.dlq"
    assert dead_letter["reason"] == "Unknown product identifier: MISSING-SKU"


@pytest.mark.asyncio
async def test_expired_reservation_releases_atp_without_changing_on_hand(
    session_factory: async_sessionmaker,
) -> None:
    product_id = await add_product(session_factory, stock=5)
    async with session_factory() as session:
        session.add(
            InventoryReservation(
                event_id="evt-expired",
                reservation_id="res-expired",
                order_id="order-expired",
                product_id=product_id,
                quantity=2,
                status="reserved",
                expires_at=datetime.now(timezone.utc) - timedelta(minutes=1),
            )
        )
        product = await session.get(Product, product_id)
        assert product is not None
        product.reserved_quantity = 2
        await session.commit()

    assert (
        await release_expired_reservations_once(
            session_factory,
            "domain.events",
        )
        == 1
    )

    async with session_factory() as session:
        product = await session.get(Product, product_id)
        reservation = await session.scalar(select(InventoryReservation))
        expired_event = await session.scalar(
            select(OutboxEvent).where(
                OutboxEvent.event_type == "inventory.reservation_expired.v1"
            )
        )
        assert product is not None
        assert product.stock == 5
        assert product.reserved_quantity == 0
        assert reservation is not None
        assert reservation.status == "expired"
        assert expired_event is not None
