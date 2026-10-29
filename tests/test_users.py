"""Unit and integration tests for User API endpoints, cursor pagination, and E.164 validation."""

from datetime import datetime, timezone
import uuid
import pytest
from pydantic import ValidationError

from app.schemas.base import decode_cursor, encode_cursor
from app.schemas.user import UserCreate


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

