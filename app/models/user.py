"""User domain dataclass models.

Includes User, Department, Role, UserAccount, and aggregated UserProfile.
Plain dataclasses with no ORM, no declarative mapping.
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional
import uuid


@dataclass
class Department:
    id: int
    name: str
    code: str
    description: Optional[str] = None
    created_at: Optional[datetime] = None


@dataclass
class Role:
    id: int
    name: str
    description: Optional[str] = None
    permissions: List[str] = field(default_factory=list)
    created_at: Optional[datetime] = None


@dataclass
class UserAccount:
    id: uuid.UUID
    user_id: uuid.UUID
    account_number: str
    account_type: str = "checking"
    balance_cents: int = 0
    currency: str = "USD"
    status: str = "active"
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


@dataclass
class User:
    id: uuid.UUID
    name: str
    email: str
    role: str
    phone_encrypted: str
    phone_blind_index: str
    department_id: Optional[int] = None
    role_id: Optional[int] = None
    created_at: Optional[datetime] = None

    def __repr__(self) -> str:
        return f"<User id={self.id} email={self.email} role={self.role}>"


@dataclass
class UserProfile:
    user: User
    department: Optional[Department] = None
    role_info: Optional[Role] = None
    accounts: List[UserAccount] = field(default_factory=list)
    total_balance_cents: int = 0


__all__ = [
    "Department",
    "Role",
    "UserAccount",
    "User",
    "UserProfile",
]
