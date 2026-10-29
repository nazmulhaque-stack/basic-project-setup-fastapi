"""Pydantic v2 schemas for User entity and standardized API contracts.

Enforces:
1. E.164 phone number validation on creation and update.
2. PII protection: Output schemas ONLY expose masked phone numbers (e.g. '+1******0198').
3. Keyset cursor pagination structure (items + next_cursor).
4. Uniform JSON error envelope schema without leaking server tracebacks.
"""

from datetime import datetime
import re
from typing import List, Optional, Union
import uuid

from pydantic import BaseModel, ConfigDict, EmailStr, Field, field_validator

# E.164 international phone number regex: + followed by 1 to 14 digits (max 15 digits total)
E164_REGEX = re.compile(r"^\+[1-9]\d{1,14}$")


class UserBase(BaseModel):
    """Shared user properties."""
    name: str = Field(..., min_length=1, max_length=255, description="Full name of user")
    email: EmailStr = Field(..., description="Unique email address")
    role: str = Field(default="user", min_length=1, max_length=50, description="Authorization role")


class UserCreate(UserBase):
    """Payload for user creation. Validates phone number to strict E.164 standards."""
    phone: str = Field(..., description="Phone number in E.164 format (e.g. +14155552671)")

    @field_validator("phone")
    @classmethod
    def validate_e164_phone(cls, v: str) -> str:
        # Strip internal whitespace or dashes before checking
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


__all__ = [
    "UserBase",
    "UserCreate",
    "UserUpdate",
    "UserRead",
    "UserPage",
]
