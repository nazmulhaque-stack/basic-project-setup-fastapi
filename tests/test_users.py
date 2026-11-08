"""Unit and integration tests for User API endpoints, cursor pagination, profile schemas, and E.164 validation."""

from datetime import datetime, timezone
import uuid
import pytest
from pydantic import ValidationError

from app.schemas.base import decode_cursor, encode_cursor
from app.schemas.outbox import OutboxEventRead
from app.schemas.user import (
    AccountCreate,
    AccountRead,
    DepartmentRead,
    RoleRead,
    UserCreate,
    UserProfileRead,
)


def test_cursor_encoding_and_decoding() -> None:
    """Verifies keyset cursor encoding to Base64 and decoding back to exact timestamp and UUID."""
    now: datetime = datetime.now(timezone.utc)
    item_id: uuid.UUID = uuid.uuid4()

    token: str = encode_cursor(now, item_id)
    assert isinstance(token, str)

    decoded_dt: datetime
    decoded_id: uuid.UUID
    decoded_dt, decoded_id = decode_cursor(token)
    assert decoded_id == item_id
    assert decoded_dt == now


def test_user_create_phone_e164_validation() -> None:
    """Verifies that phone numbers are strictly validated against E.164 rules."""
    # Valid E.164
    user_valid: UserCreate = UserCreate(
        name="John Doe",
        email="john@example.com",
        phone="+14155552671",
        role="user",
    )
    assert user_valid.phone == "+14155552671"

    # Valid with formatting spaces/dashes (normalized)
    user_formatted: UserCreate = UserCreate(
        name="Jane Doe",
        email="jane@example.com",
        phone="+44 20 7946 0991",
        role="admin",
    )
    assert user_formatted.phone == "+442079460991"

    # Invalid: Missing leading +
    with pytest.raises(ValidationError):
        UserCreate(name="Bad", email="bad@example.com", phone="14155552671")

    # Invalid: Alphabetic characters
    with pytest.raises(ValidationError):
        UserCreate(name="Bad", email="bad@example.com", phone="+1415555abcd")

    # Invalid: Exceeds 15 digits
    with pytest.raises(ValidationError):
        UserCreate(name="Bad", email="bad@example.com", phone="+12345678901234567")


def test_user_profile_schemas() -> None:
    """Verifies rich UserProfileRead and Account schemas."""
    uid = uuid.uuid4()
    acc_id = uuid.uuid4()

    account = AccountRead(
        id=acc_id,
        account_number="ACC-123456",
        account_type="savings",
        balance_cents=250000,
        currency="USD",
        status="active",
        created_at=datetime.now(timezone.utc),
    )

    profile = UserProfileRead(
        id=uid,
        name="Alice Admin",
        email="alice@company.com",
        role="admin",
        phone_masked="+1******0199",
        department=DepartmentRead(id=1, name="Engineering", code="ENG", description="Software dev"),
        role_info=RoleRead(id=1, name="Admin", description="Superuser", permissions=["admin:all"]),
        accounts=[account],
        total_balance_cents=250000,
        account_count=1,
    )

    assert profile.id == uid
    assert profile.department.code == "ENG"
    assert profile.role_info.permissions == ["admin:all"]
    assert profile.accounts[0].account_number == "ACC-123456"
    assert profile.total_balance_cents == 250000
    assert profile.account_count == 1


def test_user_create_with_zero_department_and_role_id() -> None:
    """Verifies that Swagger default zeros for department_id and role_id are safely handled without FK crashes."""
    user = UserCreate.model_validate({
        "name": "string",
        "email": "user2@example.com",
        "role": "user",
        "department_id": 0,
        "role_id": 0,
        "phone": "+14155552673",
        "accounts": [
            {
                "account_number": "ACC-TEST-001",
                "account_type": "checking",
                "balance_cents": 0,
                "currency": "USD",
                "status": "active",
            }
        ],
    })
    assert user.name == "string"
    assert user.email == "user2@example.com"
    # department_id = 0 should be stripped/ignored so it doesn't cause foreign key 0 error
    assert user.department is None
    assert user.phone == "+14155552673"
    assert len(user.accounts) == 1
    assert user.accounts[0].account_number == "ACC-TEST-001"


def test_user_create_with_string_department() -> None:
    """Verifies that text/string department names are stored correctly."""
    user = UserCreate.model_validate({
        "name": "Sarah Connor",
        "email": "sarah@cyberdyne.com",
        "role": "Administrator",
        "department": "Engineering",
        "phone": "+14155552674",
    })
    assert user.department == "Engineering"
    assert user.role == "Administrator"


def test_outbox_event_schema() -> None:
    """Verifies Transactional Outbox schema and serialization."""
    event_id = uuid.uuid4()
    now = datetime.now(timezone.utc)
    event = OutboxEventRead(
        id=event_id,
        aggregate_type="user",
        aggregate_id=str(uuid.uuid4()),
        event_type="CREATE",
        payload={"user": {"name": "Test User"}, "profile": {}},
        status="PUBLISHED",
        retry_count=0,
        last_error=None,
        created_at=now,
        published_at=now,
    )
    assert event.id == event_id
    assert event.status == "PUBLISHED"
    assert event.published_at == now
    assert event.payload["user"]["name"] == "Test User"


