"""Tests for reservation event idempotency, availability, and expiration.

Integration tests here run against a real Postgres connection via the `pg_pool`
fixture (see `tests/conftest.py`) and self-clean their rows, since there is no
in-memory substitute that faithfully reproduces asyncpg/Postgres behavior (row
locking, `FOR UPDATE SKIP LOCKED`, conditional atomic UPDATEs) now that the
project talks to the database only via raw asyncpg (no SQLAlchemy, no SQLite
fallback).
"""

from datetime import datetime, timedelta, timezone
from typing import Any
import uuid

import pytest

from app.services.base import format_event_type
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


async def add_product(pool: Any, *, sku: str, stock: int = 5) -> uuid.UUID:
    product_id = uuid.uuid4()
    async with pool.acquire() as connection:
        await connection.execute(
            """
            INSERT INTO products (id, name, sku, price_cents, category, is_active, stock, reserved_quantity)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            """,
            product_id,
            "Widget",
            sku,
            100,
            "general",
            True,
            stock,
            0,
        )
    return product_id


async def cleanup_product(pool: Any, product_id: uuid.UUID) -> None:
    async with pool.acquire() as connection:
        await connection.execute(
            "DELETE FROM inventory_reservations WHERE product_id = $1", product_id
        )
        await connection.execute(
            "DELETE FROM outbox_events WHERE event_key = $1", str(product_id)
        )
        await connection.execute("DELETE FROM products WHERE id = $1", product_id)


@pytest.mark.asyncio
async def test_reservation_reduces_available_stock_but_preserves_on_hand(pg_pool: Any) -> None:
    sku = f"SKU-{uuid.uuid4().hex[:8]}"
    product_id = await add_product(pg_pool, sku=sku)
    try:
        producer = FakeProducer()
        event = reservation_event(event_id=f"evt-{sku}", reservation_id=f"res-{sku}", product_id=sku)

        await handle_inventory_reserved(
            event,
            pg_pool,
            producer,
            output_topic="domain.events",
            dead_letter_topic="inventory.events.dlq",
        )

        async with pg_pool.acquire() as connection:
            product = await connection.fetchrow("SELECT * FROM products WHERE id = $1", product_id)
            reservation = await connection.fetchrow(
                "SELECT * FROM inventory_reservations WHERE product_id = $1", product_id
            )
            assert product is not None
            assert product["stock"] == 5
            assert product["reserved_quantity"] == 2
            assert product["stock"] - product["reserved_quantity"] == 3
            assert reservation is not None
            assert reservation["status"] == "reserved"
            assert reservation["location_id"] == "warehouse-east"
            assert reservation["expires_at"] is not None

        # Re-delivering the same event is idempotent (same reservation_id/event_id)
        await handle_inventory_reserved(
            event,
            pg_pool,
            producer,
            output_topic="domain.events",
            dead_letter_topic="inventory.events.dlq",
        )
        async with pg_pool.acquire() as connection:
            product = await connection.fetchrow("SELECT * FROM products WHERE id = $1", product_id)
            count = await connection.fetchval(
                "SELECT count(*) FROM inventory_reservations WHERE product_id = $1", product_id
            )
            assert product["stock"] == 5
            assert product["reserved_quantity"] == 2
            assert count == 1
    finally:
        await cleanup_product(pg_pool, product_id)


@pytest.mark.asyncio
async def test_insufficient_stock_records_rejection_and_compensation(pg_pool: Any) -> None:
    sku = f"SKU-{uuid.uuid4().hex[:8]}"
    product_id = await add_product(pg_pool, sku=sku, stock=1)
    try:
        producer = FakeProducer()
        event = reservation_event(
            event_id=f"evt-{sku}", reservation_id=f"res-{sku}", product_id=sku, quantity=2
        )

        # Two deliveries of the SAME event: idempotency means only one reservation row.
        await handle_inventory_reserved(
            event,
            pg_pool,
            producer,
            output_topic="domain.events",
            dead_letter_topic="inventory.events.dlq",
        )
        await handle_inventory_reserved(
            event,
            pg_pool,
            producer,
            output_topic="domain.events",
            dead_letter_topic="inventory.events.dlq",
        )

        async with pg_pool.acquire() as connection:
            product = await connection.fetchrow("SELECT * FROM products WHERE id = $1", product_id)
            reservation = await connection.fetchrow(
                "SELECT * FROM inventory_reservations WHERE product_id = $1", product_id
            )
            outbox_events = await connection.fetch(
                "SELECT * FROM outbox_events WHERE event_type = $1",
                format_event_type("inventory", "reservation_failed", "v1"),
            )
            outbox_events = [r for r in outbox_events if r["event_key"] == str(product_id)]
            assert product is not None
            assert product["stock"] == 1
            assert product["reserved_quantity"] == 0
            assert reservation is not None
            assert reservation["status"] == "rejected"
            assert len(outbox_events) == 1
    finally:
        await cleanup_product(pg_pool, product_id)


@pytest.mark.asyncio
async def test_unknown_product_is_dead_lettered(pg_pool: Any) -> None:
    producer = FakeProducer()
    await handle_inventory_reserved(
        reservation_event(product_id="MISSING-SKU-DOES-NOT-EXIST"),
        pg_pool,
        producer,
        output_topic="domain.events",
        dead_letter_topic="inventory.events.dlq",
    )

    assert len(producer.events) == 1
    topic, _, dead_letter = producer.events[0]
    assert topic == "inventory.events.dlq"
    assert dead_letter["reason"] == "Unknown product identifier: MISSING-SKU-DOES-NOT-EXIST"


@pytest.mark.asyncio
async def test_expired_reservation_releases_atp_without_changing_on_hand(pg_pool: Any) -> None:
    sku = f"SKU-{uuid.uuid4().hex[:8]}"
    product_id = await add_product(pg_pool, sku=sku, stock=5)
    try:
        async with pg_pool.acquire() as connection:
            await connection.execute(
                "UPDATE products SET reserved_quantity = 2 WHERE id = $1", product_id
            )
            await connection.execute(
                """
                INSERT INTO inventory_reservations
                    (id, event_id, reservation_id, order_id, product_id, quantity, status, expires_at)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                """,
                uuid.uuid4(),
                f"evt-expired-{sku}",
                f"res-expired-{sku}",
                "order-expired",
                product_id,
                2,
                "reserved",
                datetime.now(timezone.utc) - timedelta(minutes=1),
            )

        assert (
            await release_expired_reservations_once(
                pg_pool,
                "domain.events",
            )
            >= 1
        )

        async with pg_pool.acquire() as connection:
            product = await connection.fetchrow("SELECT * FROM products WHERE id = $1", product_id)
            reservation = await connection.fetchrow(
                "SELECT * FROM inventory_reservations WHERE product_id = $1", product_id
            )
            expired_event = await connection.fetchrow(
                "SELECT * FROM outbox_events WHERE event_type = $1 AND event_key = $2",
                format_event_type("inventory", "reservation_expired", "v1"),
                str(product_id),
            )
            assert product is not None
            assert product["stock"] == 5
            assert product["reserved_quantity"] == 0
            assert reservation is not None
            assert reservation["status"] == "expired"
            assert expired_event is not None
    finally:
        await cleanup_product(pg_pool, product_id)
