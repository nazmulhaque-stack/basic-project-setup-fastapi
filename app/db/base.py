"""Declarative base class + reusable mixins (SQLAlchemy 2.x typed style)."""

from datetime import datetime
import uuid

from sqlalchemy import DateTime, MetaData, func
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.ext.asyncio import AsyncAttrs
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

# Deterministic constraint naming convention for clean Alembic migrations
NAMING_CONVENTION = {
    "ix": "ix_%(column_0_label)s",
    "uq": "uq_%(table_name)s_%(column_0_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}


class Base(AsyncAttrs, DeclarativeBase):
    """Base declarative class for all application ORM entities."""

    metadata = MetaData(naming_convention=NAMING_CONVENTION)

    def __repr__(self) -> str:
        pk = getattr(self, "id", None)
        return f"<{type(self).__name__} id={pk}>"


class IdIntMixin:
    """Integer primary key mixin."""

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True, sort_order=-100)


class IdUuidMixin:
    """UUID primary key mixin."""

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
        sort_order=-100,
    )


# Alias IdMixin to IdIntMixin for compatibility
IdMixin = IdIntMixin


class TimestampMixin:
    """Automatic server-side UTC timestamps."""

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
        sort_order=100,
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
        sort_order=101,
    )


__all__ = [
    "Base",
    "NAMING_CONVENTION",
    "IdMixin",
    "IdIntMixin",
    "IdUuidMixin",
    "TimestampMixin",
]
