"""Idempotent handling and expiration of inventory reservation events (raw asyncpg)."""

import asyncio
from datetime import datetime, timedelta, timezone
import logging
import uuid
from typing import Any, Optional

import asyncpg
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from app.core.kafka import KafkaEventProducer
from app.services.base import format_event_type

logger = logging.getLogger(__name__)


class InventoryReservedPayload(BaseModel):
    """Validated payload for a stock reservation request."""

    model_config = ConfigDict(extra="ignore")

    reservation_id: str = Field(min_length=1, max_length=255)
    order_id: str = Field(min_length=1, max_length=255)
    product_id: str = Field(min_length=1, max_length=255)
    quantity: int = Field(gt=0)
    location_id: str | None = Field(default=None, max_length=255)
    ttl_minutes: int = Field(gt=0, le=1440)


class InventoryReservedEvent(BaseModel):
    """Versioned inventory reservation event envelope."""

    model_config = ConfigDict(extra="ignore")

    event_id: str = Field(min_length=1, max_length=255)
    event_type: str
    timestamp: datetime
    payload: InventoryReservedPayload


def _dead_letter_payload(event: dict[str, Any], reason: str) -> dict[str, Any]:
    return {
        "event_id": event.get("event_id"),
        "event_type": event.get("event_type"),
        "reason": reason,
        "original_event": event,
        "dead_lettered_at": datetime.now(timezone.utc).isoformat(),
    }


async def _dead_letter(
    event: dict[str, Any],
    reason: str,
    producer: KafkaEventProducer,
    dead_letter_topic: str,
) -> None:
    event_key = str(
        event.get("event_id")
        or (event.get("payload") or {}).get("reservation_id")
        or "invalid-inventory-event"
    )
    await producer.publish(
        dead_letter_topic,
        event_key,
        _dead_letter_payload(event, reason),
    )


async def _insert_outbox_event(
    connection: asyncpg.Connection,
    *,
    topic: str,
    event_key: str,
    event_type: str,
    payload: dict[str, Any],
) -> None:
    await connection.execute(
        """
        INSERT INTO outbox_events (id, topic, event_key, event_type, payload)
        VALUES ($1, $2, $3, $4, $5)
        """,
        uuid.uuid4(),
        topic,
        event_key,
        event_type,
        payload,
    )


async def handle_inventory_reserved(
    event: dict[str, Any],
    pool: asyncpg.Pool,
    producer: KafkaEventProducer,
    *,
    output_topic: str,
    dead_letter_topic: str,
) -> None:
    """Move ATP into reserved inventory, or emit a rejection/DLQ event.

    `products.stock` remains physical on-hand quantity. The conditional update
    reserves only when stock minus existing reservations can cover the request.
    """
    try:
        envelope = InventoryReservedEvent.model_validate(event)
        # Accept versioned identifier or developer convention <service>.<event>.<event name>
        is_convention = (
            envelope.event_type.startswith("inventory.")
            and len(envelope.event_type.split(".")) >= 3
        )
        if envelope.event_type != "inventory.reserved.v1" and not is_convention:
            raise ValueError(f"Unsupported inventory event type: {envelope.event_type}")
    except (ValidationError, ValueError) as exc:
        await _dead_letter(event, str(exc), producer, dead_letter_topic)
        logger.critical("Dead-lettered invalid inventory reservation event: %s", exc)
        return

    payload = envelope.payload
    product_uuid: Optional[uuid.UUID]
    try:
        product_uuid = uuid.UUID(payload.product_id)
    except ValueError:
        product_uuid = None

    dead_letter_reason: Optional[str] = None
    sku = payload.product_id.strip().upper()

    async with pool.acquire() as connection:
        async with connection.transaction():
            existing = await connection.fetchrow(
                """
                SELECT * FROM inventory_reservations
                WHERE reservation_id = $1 OR event_id = $2
                """,
                payload.reservation_id,
                envelope.event_id,
            )
            if existing is not None:
                existing_product = await connection.fetchrow(
                    "SELECT * FROM products WHERE id = $1", existing["product_id"]
                )
                same_request = (
                    existing["reservation_id"] == payload.reservation_id
                    and existing["order_id"] == payload.order_id
                    and existing["quantity"] == payload.quantity
                    and existing["location_id"] == payload.location_id
                    and (
                        existing["product_id"] == product_uuid
                        or (
                            product_uuid is None
                            and existing_product is not None
                            and existing_product["sku"] == sku
                        )
                    )
                )
                if same_request:
                    return
                dead_letter_reason = (
                    "event_id or reservation_id was reused with a different payload"
                )
            else:
                if product_uuid is not None:
                    product = await connection.fetchrow(
                        "SELECT * FROM products WHERE id = $1 OR sku = $2",
                        product_uuid,
                        sku,
                    )
                else:
                    product = await connection.fetchrow(
                        "SELECT * FROM products WHERE sku = $1", sku
                    )

                if product is None:
                    dead_letter_reason = f"Unknown product identifier: {payload.product_id}"
                else:
                    now = datetime.now(timezone.utc)
                    expires_at = now + timedelta(minutes=payload.ttl_minutes)
                    result = await connection.execute(
                        """
                        UPDATE products
                        SET reserved_quantity = reserved_quantity + $2, updated_at = now()
                        WHERE id = $1 AND stock - reserved_quantity >= $2
                        """,
                        product["id"],
                        payload.quantity,
                    )
                    is_reserved = result.endswith(" 1")

                    await connection.execute(
                        """
                        INSERT INTO inventory_reservations
                            (id, event_id, reservation_id, order_id, product_id,
                             quantity, location_id, status, expires_at)
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                        """,
                        uuid.uuid4(),
                        envelope.event_id,
                        payload.reservation_id,
                        payload.order_id,
                        product["id"],
                        payload.quantity,
                        payload.location_id,
                        "reserved" if is_reserved else "rejected",
                        expires_at if is_reserved else None,
                    )

                    if not is_reserved:
                        failure_type = format_event_type("inventory", "reservation_failed", "v1")
                        failure_id = uuid.uuid4()
                        available = product["stock"] - product["reserved_quantity"]
                        await _insert_outbox_event(
                            connection,
                            topic=output_topic,
                            event_key=str(product["id"]),
                            event_type=failure_type,
                            payload={
                                "event_id": str(failure_id),
                                "event_type": failure_type,
                                "timestamp": now.isoformat(),
                                "payload": {
                                    "reservation_id": payload.reservation_id,
                                    "order_id": payload.order_id,
                                    "product_id": payload.product_id,
                                    "quantity": payload.quantity,
                                    "location_id": payload.location_id,
                                    "reason": "insufficient_stock",
                                },
                            },
                        )
                        logger.warning(
                            "Rejected reservation %s: requested %d, available %d.",
                            payload.reservation_id,
                            payload.quantity,
                            available,
                        )

    if dead_letter_reason is not None:
        await _dead_letter(event, dead_letter_reason, producer, dead_letter_topic)
        logger.critical(
            "Dead-lettered inventory reservation %s: %s",
            payload.reservation_id,
            dead_letter_reason,
        )


async def release_expired_reservations_once(
    pool: asyncpg.Pool,
    output_topic: str,
    *,
    batch_size: int = 100,
) -> int:
    """Release one batch of expired reservations without changing on-hand stock."""
    released = 0
    now = datetime.now(timezone.utc)
    async with pool.acquire() as connection:
        async with connection.transaction():
            reservations = await connection.fetch(
                """
                SELECT * FROM inventory_reservations
                WHERE status = 'reserved' AND expires_at <= $1
                ORDER BY expires_at
                LIMIT $2
                FOR UPDATE SKIP LOCKED
                """,
                now,
                batch_size,
            )
            for reservation in reservations:
                result = await connection.execute(
                    """
                    UPDATE products
                    SET reserved_quantity = reserved_quantity - $2
                    WHERE id = $1 AND reserved_quantity >= $2
                    """,
                    reservation["product_id"],
                    reservation["quantity"],
                )
                if not result.endswith(" 1"):
                    raise RuntimeError(
                        "Could not release expired reservation "
                        f"{reservation['reservation_id']}; inventory state is inconsistent."
                    )

                await connection.execute(
                    "UPDATE inventory_reservations SET status = 'expired' WHERE id = $1",
                    reservation["id"],
                )

                event_type = format_event_type("inventory", "reservation_expired", "v1")
                event_id = uuid.uuid4()
                await _insert_outbox_event(
                    connection,
                    topic=output_topic,
                    event_key=str(reservation["product_id"]),
                    event_type=event_type,
                    payload={
                        "event_id": str(event_id),
                        "event_type": event_type,
                        "timestamp": now.isoformat(),
                        "payload": {
                            "reservation_id": reservation["reservation_id"],
                            "order_id": reservation["order_id"],
                            "product_id": str(reservation["product_id"]),
                            "quantity": reservation["quantity"],
                            "location_id": reservation["location_id"],
                            "reason": "ttl_expired",
                        },
                    },
                )
                released += 1
    return released


async def run_expiration_sweeper(
    pool: asyncpg.Pool,
    output_topic: str,
    *,
    interval: float,
    stop_event: asyncio.Event,
) -> None:
    """Periodically release expired reservations until application shutdown."""
    while not stop_event.is_set():
        try:
            released = await release_expired_reservations_once(pool, output_topic)
            if released:
                logger.info("Released %d expired inventory reservation(s).", released)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Inventory reservation expiration sweep failed.")
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=interval)
        except TimeoutError:
            pass


__all__ = [
    "handle_inventory_reserved",
    "release_expired_reservations_once",
    "run_expiration_sweeper",
]
