"""SQLAlchemy 2.0 mapped Employee entity with a 1-to-1 relationship to User."""

from datetime import datetime
from typing import TYPE_CHECKING, Optional
import uuid

from sqlalchemy import DateTime, ForeignKey, Index, Integer, String, Uuid, func
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.models.base import Base

if TYPE_CHECKING:
    from app.models.user import User


class Employee(Base):
    """Employee entity storing employee profile and 1-to-1 link to a User."""

    __tablename__ = "employees"

    id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
        nullable=False,
    )

    name: Mapped[str] = mapped_column(
        String(255),
        nullable=False,
        index=True,
    )

    age: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
    )

    location: Mapped[str] = mapped_column(
        String(255),
        nullable=False,
        index=True,
    )

    user_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("users.id", ondelete="CASCADE"),
        unique=True,
        nullable=False,
        index=True,
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )

    # 1-to-1 bidirectional relationship with User
    user: Mapped["User"] = relationship(
        "User",
        back_populates="employee",
        lazy="selectin",
    )

    __table_args__ = (
        Index("ix_employees_created_at_id", "created_at", "id"),
    )

    def __repr__(self) -> str:
        return f"<Employee id={self.id} name={self.name} age={self.age} location={self.location} user_id={self.user_id}>"
