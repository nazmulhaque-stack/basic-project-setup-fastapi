"""User row dataclass.

Design Rules:
- Primary key (UUIDv4) is generated client-side in the application so the UUID
  can be bound immediately into encryption Associated Authenticated Data (AAD)
  before the INSERT is sent.
- Plaintext phone numbers are NEVER stored or persisted.
- phone_blind_index holds a HMAC-SHA256 hex string with a UNIQUE index, for
  O(1) deduplication and lookups without ever decrypting rows.
- Composite index (created_at, id) powers keyset pagination (see UserRepository).
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Optional
import uuid


@dataclass
class User:
    id: uuid.UUID
    name: str
    email: str
    role: str
    phone_encrypted: str
    phone_blind_index: str
    created_at: Optional[datetime] = None

    def __repr__(self) -> str:
        return f"<User id={self.id} email={self.email} role={self.role}>"


__all__ = ["User"]
