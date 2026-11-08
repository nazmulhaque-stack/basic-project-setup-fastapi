"""Pydantic v2 schemas for User entity, Department, Role, Accounts, and UserProfile.

Enforces:
1. E.164 phone number validation on creation and update.
2. PII protection: Output schemas ONLY expose masked phone numbers (e.g. '+1******0198').
3. Keyset cursor pagination structure (items + next_cursor).
4. Rich UserProfileRead combining User, Department, Role, and UserAccounts for complex queries.
5. Text/string based relations (no database foreign key constraints).
"""

from datetime import datetime
import re
from typing import Any, List, Optional, Union
import uuid

from pydantic import BaseModel, ConfigDict, EmailStr, Field, field_validator, model_validator

# E.164 international phone number regex: + followed by 1 to 14 digits (max 15 digits total)
E164_REGEX = re.compile(r"^\+[1-9]\d{1,14}$")


class DepartmentRead(BaseModel):
    """Department public contract."""
    id: Optional[int] = None
    name: str = ""
    code: str = ""
    description: Optional[str] = None

    model_config = ConfigDict(from_attributes=True)


class RoleRead(BaseModel):
    """Role and permissions public contract."""
    id: Optional[int] = None
    name: str = ""
    description: Optional[str] = None
    permissions: List[str] = Field(default_factory=list)

    model_config = ConfigDict(from_attributes=True)


class AccountCreate(BaseModel):
    """Account creation contract."""
    account_number: str = Field(..., min_length=1, max_length=64, description="Unique financial or profile account number", examples=["ACC-CORP-1001"])
    account_type: str = Field(default="checking", description="Account type, e.g. checking, savings, corporate", examples=["corporate"])
    balance_cents: int = Field(default=0, ge=0, description="Balance in cents", examples=[500000])
    currency: str = Field(default="USD", min_length=3, max_length=3, description="ISO-4217 3-letter currency code", examples=["USD"])
    status: str = Field(default="active", description="Account status: active, frozen, closed", examples=["active"])

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "account_number": "ACC-CORP-1001",
                "account_type": "corporate",
                "balance_cents": 500000,
                "currency": "USD",
                "status": "active",
            }
        }
    )


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
    """Shared user properties with text/string relations."""
    name: str = Field(..., min_length=1, max_length=255, description="Full name of user", examples=["Sarah Connor"])
    email: EmailStr = Field(..., description="Unique email address", examples=["sarah.connor@example.com"])
    role: str = Field(default="user", min_length=1, max_length=50, description="Authorization role name (e.g. Administrator, Manager, user)", examples=["Administrator"])
    department: Optional[str] = Field(default=None, description="Department name or code (text/string)", examples=["Engineering"])
    department_id: Optional[Union[str, int]] = Field(default=None, description="Alternative department identifier (string or int)", examples=["ENG"])
    role_id: Optional[Union[str, int]] = Field(default=None, description="Alternative role identifier (string or int)", examples=["Administrator"])

    @model_validator(mode="before")
    @classmethod
    def normalize_department_and_role(cls, data: Any) -> Any:
        if isinstance(data, dict):
            # Normalize department
            dept_val = data.get("department")
            dept_id = data.get("department_id")
            if not dept_val and dept_id is not None:
                cleaned_id = str(dept_id).strip()
                if cleaned_id not in ("0", ""):
                    data["department"] = cleaned_id
            elif dept_val:
                data["department"] = str(dept_val).strip()

            # Normalize role
            role_val = data.get("role")
            role_id = data.get("role_id")
            if (not role_val or role_val == "user") and role_id is not None:
                cleaned_role_id = str(role_id).strip()
                if cleaned_role_id not in ("0", ""):
                    data["role"] = cleaned_role_id
        return data


class UserCreate(UserBase):
    """Payload for user creation. Validates phone number to strict E.164 standards."""
    phone: str = Field(..., description="Phone number in E.164 format (e.g. +14155552671)", examples=["+14155552671"])
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

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "name": "Sarah Connor",
                "email": "sarah.connor@example.com",
                "role": "Administrator",
                "department": "Engineering",
                "phone": "+14155552671",
                "accounts": [
                    {
                        "account_number": "ACC-CORP-1001",
                        "account_type": "corporate",
                        "balance_cents": 500000,
                        "currency": "USD",
                        "status": "active"
                    }
                ]
            }
        }
    )


class UserUpdate(BaseModel):
    """Payload for partial user updates."""
    name: Optional[str] = Field(default=None, min_length=1, max_length=255, examples=["Sarah Connor"])
    email: Optional[EmailStr] = Field(default=None, description="Updated unique email address", examples=["sarah.connor@example.com"])
    phone: Optional[str] = Field(default=None, description="New phone number in E.164 format", examples=["+14155558899"])
    role: Optional[str] = Field(default=None, min_length=1, max_length=50, examples=["Manager"])
    department: Optional[str] = Field(default=None, description="Department name or code", examples=["Finance"])
    department_id: Optional[Union[str, int]] = Field(default=None, description="Department identifier", examples=["FIN"])
    role_id: Optional[Union[str, int]] = Field(default=None, description="Role identifier", examples=["Manager"])

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "name": "Sarah Connor",
                "department": "Finance",
                "role": "Manager",
                "phone": "+14155558899"
            }
        }
    )

    @model_validator(mode="before")
    @classmethod
    def normalize_department_and_role(cls, data: Any) -> Any:
        if isinstance(data, dict):
            dept_val = data.get("department")
            dept_id = data.get("department_id")
            if not dept_val and dept_id is not None:
                cleaned_id = str(dept_id).strip()
                if cleaned_id not in ("0", ""):
                    data["department"] = cleaned_id
            elif dept_val:
                data["department"] = str(dept_val).strip()

            role_id = data.get("role_id")
            if role_id is not None:
                cleaned_role_id = str(role_id).strip()
                if cleaned_role_id not in ("0", ""):
                    if not data.get("role"):
                        data["role"] = cleaned_role_id
        return data

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
    department: Optional[str] = None
    department_id: Optional[Union[str, int]] = None
    role_id: Optional[Union[str, int]] = None
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
    department: Optional[DepartmentRead] = None
    role_info: Optional[RoleRead] = None
    accounts: List[AccountRead] = Field(default_factory=list)
    total_balance_cents: int = 0
    account_count: int = 0
    phone_masked: Optional[str] = None
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
