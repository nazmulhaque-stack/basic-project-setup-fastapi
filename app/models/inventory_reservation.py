"""Persistent inventory reservations consumed from Kafka."""

from datetime import datetime
import uuid

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    Uuid,
    UniqueConstraint,
    func,
)
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class InventoryReservation(Base):
    """Idempotent reservation record with a bounded expiration time."""

    __tablename__ = "inventory_reservations"
    __table_args__ = (
        UniqueConstraint("reservation_id", name="uq_inventory_reservations_reservation_id"),
        UniqueConstraint("event_id", name="uq_inventory_reservations_event_id"),
        CheckConstraint("quantity > 0", name="quantity_positive"),
        CheckConstraint(
            "status IN ('reserved', 'rejected', 'expired')",
            name="valid_status",
        ),
        Index(
            "ix_inventory_reservations_expiration",
            "status",
            "expires_at",
        ),
        Index("ix_inventory_reservations_created_at", "created_at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
    )
    event_id: Mapped[str] = mapped_column(String(255), nullable=False)
    reservation_id: Mapped[str] = mapped_column(String(255), nullable=False)
    order_id: Mapped[str] = mapped_column(String(255), nullable=False)
    product_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("products.id", ondelete="RESTRICT"),
        nullable=False,
        index=True,
    )
    quantity: Mapped[int] = mapped_column(Integer, nullable=False)
    location_id: Mapped[str | None] = mapped_column(String(255))
    status: Mapped[str] = mapped_column(String(16), nullable=False)
    expires_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )


__all__ = ["InventoryReservation"]
