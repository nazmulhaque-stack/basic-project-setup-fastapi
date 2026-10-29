"""SQLAlchemy 2.0 mapped User entity.

Design Rules:
- Primary key (UUIDv4) is generated client-side in the application so that the UUID
  can be bound immediately into encryption Associated Authenticated Data (AAD) before DB insertion.
- Plaintext phone numbers are NEVER stored or persisted.
- phone_blind_index holds HMAC-SHA256 hex string with a UNIQUE index to allow O(1) deduplication
  and indexed lookups without ever decrypting rows.
- Keyset composite index (created_at, id) powers high-scale pagination.
"""

from datetime import datetime
import uuid

from sqlalchemy import DateTime, Index, String, Text, func
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.models.base import Base


class User(Base):
    """User entity storing encrypted PII, blind index, and role-based metadata."""

    __tablename__ = "users"

    # Client-generated UUIDv4: needed so ID is known before insert for AAD binding
    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
        nullable=False,
    )

    name: Mapped[str] = mapped_column(
        String(255),
        nullable=False,
    )

    email: Mapped[str] = mapped_column(
        String(255),
        unique=True,
        index=True,
        nullable=False,
    )

    role: Mapped[str] = mapped_column(
        String(50),
        nullable=False,
        default="user",
        index=True,
    )

    # AES-256-GCM ciphertext in URL-safe base64: [version(1B) || nonce(12B) || ciphertext+tag(16B)]
    phone_encrypted: Mapped[str] = mapped_column(
        Text,
        nullable=False,
    )

    # Deterministic HMAC-SHA256 hash (64 hex characters) of normalized E.164 phone
    # Enables UNIQUE constraint and indexed search without exposing plaintext phone
    phone_blind_index: Mapped[str] = mapped_column(
        String(64),
        unique=True,
        index=True,
        nullable=False,
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )

    __table_args__ = (
        # Critical composite index for O(log N) keyset pagination (WHERE (created_at, id) > ...)
        Index("ix_users_created_at_id", "created_at", "id"),
    )

    def __repr__(self) -> str:
        return f"<User id={self.id} email={self.email} role={self.role}>"
