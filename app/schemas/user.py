"""Pydantic v2 schemas for User entity, Department, Role, Accounts, and UserProfile.

Enforces:
1. E.164 phone number validation on creation and update.
2. PII protection: Output schemas ONLY expose masked phone numbers (e.g. '+1******0198').
3. Keyset cursor pagination structure (items + next_cursor).
4. Rich UserProfileRead combining User, Department, Role, and UserAccounts for complex queries.
"""

from datetime import datetime
import re
from typing import List, Optional, Union
import uuid

from pydantic import BaseModel, ConfigDict, EmailStr, Field, field_validator

# E.164 international phone number regex: + followed by 1 to 14 digits (max 15 digits total)
E164_REGEX = re.compile(r"^\+[1-9]\d{1,14}$")


class DepartmentRead(BaseModel):
    """Department public contract."""
    id: int
    name: str
    code: str
    description: Optional[str] = None

    model_config = ConfigDict(from_attributes=True)


class RoleRead(BaseModel):
    """Role and permissions public contract."""
    id: int
    name: str
    description: Optional[str] = None
    permissions: List[str] = Field(default_factory=list)

    model_config = ConfigDict(from_attributes=True)


class AccountCreate(BaseModel):
    """Account creation contract."""
    account_number: str = Field(..., min_length=4, max_length=64, description="Unique financial or profile account number")
    account_type: str = Field(default="checking", description="Account type, e.g. checking, savings, corporate")
    balance_cents: int = Field(default=0, ge=0, description="Balance in cents")
    currency: str = Field(default="USD", min_length=3, max_length=3, description="ISO-4217 3-letter currency code")
    status: str = Field(default="active", description="Account status: active, frozen, closed")


class AccountRead(BaseModel):
    """Account public contract."""
    id: uuid.UUID
    account_number: str
    account_type: str
    balance_cents: int
    currency: str
    status: str
    created_at: Optional[datetime] = None

    model_config = ConfigDict(from_attributes=True)


class UserBase(BaseModel):
    """Shared user properties."""
    name: str = Field(..., min_length=1, max_length=255, description="Full name of user")
    email: EmailStr = Field(..., description="Unique email address")
    role: str = Field(default="user", min_length=1, max_length=50, description="Authorization role name")
    department_id: Optional[int] = Field(default=None, description="Optional foreign key to departments")
    role_id: Optional[int] = Field(default=None, description="Optional foreign key to roles")


class UserCreate(UserBase):
    """Payload for user creation. Validates phone number to strict E.164 standards."""
    phone: str = Field(..., description="Phone number in E.164 format (e.g. +14155552671)")
    accounts: Optional[List[AccountCreate]] = Field(default=None, description="Optional initial accounts to attach")

    @field_validator("phone")
    @classmethod
    def validate_e164_phone(cls, v: str) -> str:
        normalized = re.sub(r"[\s\-\(\)\.]", "", v.strip())
        if not E164_REGEX.match(normalized):
            raise ValueError(
                "Phone number must be valid E.164 format starting with '+' followed by country code and digits (e.g., +14155552671)"
            )
        return normalized


class UserUpdate(BaseModel):
    """Payload for partial user updates."""
    name: Optional[str] = Field(default=None, min_length=1, max_length=255)
    email: Optional[EmailStr] = Field(default=None, description="Updated unique email address")
    phone: Optional[str] = Field(default=None, description="New phone number in E.164 format")
    role: Optional[str] = Field(default=None, min_length=1, max_length=50)
    department_id: Optional[int] = Field(default=None, description="Department ID")
    role_id: Optional[int] = Field(default=None, description="Role ID")

    @field_validator("phone")
    @classmethod
    def validate_e164_phone(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return None
        normalized = re.sub(r"[\s\-\(\)\.]", "", v.strip())
        if not E164_REGEX.match(normalized):
            raise ValueError(
                "Phone number must be valid E.164 format starting with '+' (e.g., +14155552671)"
            )
        return normalized


class UserRead(BaseModel):
    """Public safe representation of a user with masked PII."""
    id: Union[uuid.UUID, int]
    name: str
    email: EmailStr
    role: str = "user"
    department_id: Optional[int] = None
    role_id: Optional[int] = None
    phone_masked: Optional[str] = Field(default=None, description="Masked phone number protecting user PII")
    created_at: Optional[datetime] = None

    model_config = ConfigDict(from_attributes=True)


class UserPage(BaseModel):
    """Cursor-paginated container for users."""
    items: List[UserRead]
    next_cursor: Optional[str] = Field(
        default=None,
        description="Opaque base64 cursor token to retrieve the next contiguous page",
    )
    limit: int = Field(default=50, ge=1, le=100)


class UserProfileRead(BaseModel):
    """Rich aggregated user profile combining User, Department, Role, and UserAccounts."""
    id: uuid.UUID
    name: str
    email: EmailStr
    role: str
    phone_masked: Optional[str] = None
    department: Optional[DepartmentRead] = None
    role_info: Optional[RoleRead] = None
    accounts: List[AccountRead] = Field(default_factory=list)
    total_balance_cents: int = 0
    account_count: int = 0
    created_at: Optional[datetime] = None

    model_config = ConfigDict(from_attributes=True)


__all__ = [
    "DepartmentRead",
    "RoleRead",
    "AccountCreate",
    "AccountRead",
    "UserBase",
    "UserCreate",
    "UserUpdate",
    "UserRead",
    "UserPage",
    "UserProfileRead",
]
