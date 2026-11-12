"""Unit and integration tests for User API endpoints, cursor pagination, profile schemas, and E.164 validation."""

from datetime import datetime, timezone
from typing import Any
from unittest.mock import AsyncMock
import uuid
import pytest
from pydantic import ValidationError

from app.models.outbox import OutboxEvent
from app.schemas.base import decode_cursor, encode_cursor
from app.schemas.outbox import OutboxEventRead
from app.schemas.user import (
    AccountRead,
    DepartmentRead,
    RoleRead,
    UserCreate,
    UserProfileRead,
)
from app.services.user import UserService


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


def _make_mocked_user_service(crypto: Any) -> UserService:
    """Create a UserService backed by async mocks for create_user unit tests."""
    db = AsyncMock()
    db.commit = AsyncMock()

    cache = AsyncMock()
    cache.invalidate_lists = AsyncMock(return_value=True)
    cache.set_item = AsyncMock(return_value=True)
    cache.delete_item = AsyncMock(return_value=True)

    es = AsyncMock()
    es.index_document = AsyncMock(return_value=True)

    service = UserService(db=db, cache=cache, crypto=crypto, elasticsearch=es)
    service.repo = AsyncMock()
    service.repo.get_by_email = AsyncMock(return_value=None)
    service.repo.get_by_phone_blind_index = AsyncMock(return_value=None)
    service.repo.get_existing_account_numbers = AsyncMock(return_value=[])
    service.repo.create = AsyncMock(side_effect=lambda u: u)
    service.repo.get_profile = AsyncMock(return_value=None)
    service.repo.get = AsyncMock()

    service.outbox_repo = AsyncMock()
    service.outbox_repo.create_event = AsyncMock(
        side_effect=lambda **kw: OutboxEvent(
            id=uuid.uuid4(),
            aggregate_type=kw["aggregate_type"],
            aggregate_id=kw["aggregate_id"],
            event_type=kw["event_type"],
            payload=kw["payload"],
            created_at=datetime.now(timezone.utc),
        )
    )
    service.outbox_repo.mark_published = AsyncMock(return_value=True)
    return service


@pytest.mark.asyncio
async def test_create_user_commits_before_cache_and_uses_outbox_fallback(
    test_crypto_service: Any,
) -> None:
    """Verifies DB commit happens before Redis sync and outbox payload avoids redundant repo.get."""
    call_order: list[str] = []
    service = _make_mocked_user_service(test_crypto_service)

    async def _commit() -> None:
        call_order.append("db.commit")

    async def _invalidate_lists() -> bool:
        call_order.append("cache.invalidate_lists")
        return True

    service.db.commit = AsyncMock(side_effect=_commit)
    service.cache.invalidate_lists = AsyncMock(side_effect=_invalidate_lists)

    payload = UserCreate(
        name="Alice Smith",
        email="alice.smith@example.com",
        phone="+14155552671",
        role="user",
    )
    result = await service.create_user(payload)

    assert result.email == "alice.smith@example.com"
    assert result.phone_masked == "+1******2671"
    assert call_order.index("db.commit") < call_order.index("cache.invalidate_lists")
    service.repo.get.assert_not_awaited()
    staged_payload = service.outbox_repo.create_event.await_args.kwargs["payload"]
    assert staged_payload["user"]["phone_masked"] == "+1******2671"


@pytest.mark.asyncio
async def test_create_user_survives_post_commit_redis_failure(
    test_crypto_service: Any,
) -> None:
    """Verifies a transient Redis failure after DB commit does not fail user creation."""
    service = _make_mocked_user_service(test_crypto_service)
    service.cache.invalidate_lists = AsyncMock(side_effect=RuntimeError("Redis connection lost"))

    result = await service.create_user(
        UserCreate(
            name="Bob Jones",
            email="bob.jones@example.com",
            phone="+14155550199",
            role="user",
        )
    )

    assert result.email == "bob.jones@example.com"
    assert result.phone_masked == "+1******0199"
    service.db.commit.assert_awaited_once()



