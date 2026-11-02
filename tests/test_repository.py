"""Unit tests for the Full Dynamic Repository Pattern and BaseService orchestration."""

from datetime import datetime, timezone
from typing import Any, Dict
from unittest.mock import AsyncMock, MagicMock
import uuid
import pytest
from sqlalchemy import select

from app.core.security import CryptoService
from app.models.user import User
from app.repositories.base import BaseRepository
from app.repositories.user import UserRepository
from app.schemas.user import UserCreate, UserPage, UserRead
from app.services.base import BaseService, DynamicRedisCache
from app.services.interface import NullCache
from app.services.user import UserService

TEST_KEY: bytes = bytes.fromhex("1111111111111111111111111111111111111111111111111111111111111111")
TEST_BLIND_KEY: bytes = bytes.fromhex("9999999999999999999999999999999999999999999999999999999999999999")


@pytest.fixture
def crypto_service() -> CryptoService:
    return CryptoService(keys={1: TEST_KEY}, active_version=1, blind_index_key=TEST_BLIND_KEY)


@pytest.fixture
def mock_user_repo() -> UserRepository:
    repo: MagicMock = MagicMock(spec=UserRepository)
    repo.model = User
    repo.session = AsyncMock()
    repo.db = repo.session
    repo.get_by_email = AsyncMock(return_value=None)
    repo.get_by_phone_blind_index = AsyncMock(return_value=None)
    return repo


@pytest.mark.asyncio
async def test_user_service_create_user(
    mock_user_repo: UserRepository,
    mock_redis: Any,
    crypto_service: CryptoService,
) -> None:
    """Verifies that create_user encrypts phone, calculates blind index, invokes repository, and invalidates list cache."""
    cache: DynamicRedisCache = DynamicRedisCache(redis=mock_redis, prefix="users")
    service: UserService = UserService(repo=mock_user_repo, cache=cache, crypto=crypto_service)

    mock_user_repo.create = AsyncMock()

    payload: UserCreate = UserCreate(
        name="Alice Architect",
        email="alice@example.com",
        phone="+14155552671",
        role="admin",
    )

    created_user: UserRead = await service.create_user(payload)

    # Repository was called once with User entity
    assert mock_user_repo.create.call_count == 1
    call_args: User = mock_user_repo.create.call_args[0][0]
    assert isinstance(call_args, User)
    assert call_args.name == "Alice Architect"
    assert call_args.email == "alice@example.com"
    # Plaintext phone is NEVER on the entity
    assert call_args.phone_encrypted != "+14155552671"
    assert len(call_args.phone_blind_index) == 64

    # Returned schema masks phone
    assert created_user.phone_masked == "+1******2671"


@pytest.mark.asyncio
async def test_user_service_get_by_id_cached(
    mock_user_repo: UserRepository,
    mock_redis: Any,
    crypto_service: CryptoService,
) -> None:
    """Verifies that get_user_by_id returns directly from cache on hit without hitting repository."""
    cache: DynamicRedisCache = DynamicRedisCache(redis=mock_redis, prefix="users")
    service: UserService = UserService(repo=mock_user_repo, cache=cache, crypto=crypto_service)

    user_id: uuid.UUID = uuid.uuid4()
    cached_record: Dict[str, Any] = {
        "id": str(user_id),
        "name": "Bob Cached",
        "email": "bob@example.com",
        "role": "user",
        "phone_masked": "+1*******9999",
        "created_at": datetime.now(timezone.utc).isoformat(),
    }

    # Populate cache
    await cache.set_item(user_id, cached_record)

    # Fetch via service
    result: UserRead = await service.get_user_by_id(user_id)

    # Assert repo was never touched
    mock_user_repo.get.assert_not_called()
    assert result.name == "Bob Cached"
    assert result.phone_masked == "+1*******9999"


@pytest.mark.asyncio
async def test_user_service_per_call_cache_bypass(
    mock_user_repo: UserRepository,
    mock_redis: Any,
    crypto_service: CryptoService,
) -> None:
    """Verifies full dynamic capability: per-call cache_enabled=False bypasses cache even if hit exists."""
    cache: DynamicRedisCache = DynamicRedisCache(redis=mock_redis, prefix="users")
    service: UserService = UserService(repo=mock_user_repo, cache=cache, crypto=crypto_service)

    user_id: uuid.UUID = uuid.uuid4()
    cached_record: Dict[str, Any] = {
        "id": str(user_id),
        "name": "Stale Cached Name",
        "email": "stale@example.com",
        "role": "user",
        "phone_masked": "+1*******0000",
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    await cache.set_item(user_id, cached_record)

    aad: bytes = f"users.phone:{user_id}".encode("utf-8")
    fresh_user: User = User(
        id=user_id,
        name="Fresh DB Name",
        email="fresh@example.com",
        role="user",
        phone_encrypted=crypto_service.encrypt("+14155559999", aad=aad),
        phone_blind_index="dummy_hash",
        created_at=datetime.now(timezone.utc),
    )
    mock_user_repo.get = AsyncMock(return_value=fresh_user)

    # Calling with cache_enabled=False bypasses the cache and fetches fresh DB entity!
    result: UserRead = await service.get_user_by_id(user_id, cache_enabled=False)
    assert result.name == "Fresh DB Name"
    assert mock_user_repo.get.call_count == 1


@pytest.mark.asyncio
async def test_user_service_list_users_invalidation(
    mock_user_repo: UserRepository,
    mock_redis: Any,
    crypto_service: CryptoService,
) -> None:
    """Verifies list caching and atomic O(1) version invalidation via repository."""
    cache: DynamicRedisCache = DynamicRedisCache(redis=mock_redis, prefix="users")
    service: UserService = UserService(repo=mock_user_repo, cache=cache, crypto=crypto_service)

    user_id: uuid.UUID = uuid.uuid4()
    aad: bytes = f"users.phone:{user_id}".encode("utf-8")
    enc_phone: str = crypto_service.encrypt("+14155551111", aad=aad)

    dummy_user: User = User(
        id=user_id,
        name="Charlie",
        email="charlie@example.com",
        role="user",
        phone_encrypted=enc_phone,
        phone_blind_index="dummy_hash",
        created_at=datetime.now(timezone.utc),
    )

    mock_user_repo.list_keyset = AsyncMock(return_value=([dummy_user], False))

    # First fetch: repository queried
    page1: UserPage = await service.list_users(limit=10)
    assert len(page1.items) == 1
    assert mock_user_repo.list_keyset.call_count == 1

    # Second fetch: served from cache (repo not queried again)
    page2: UserPage = await service.list_users(limit=10)
    assert len(page2.items) == 1
    assert mock_user_repo.list_keyset.call_count == 1  # Call count remains 1

    # Invalidate lists
    await cache.invalidate_lists()

    # Third fetch: cache invalidated, repository queried again
    page3: UserPage = await service.list_users(limit=10)
    assert len(page3.items) == 1
    assert mock_user_repo.list_keyset.call_count == 2  # Call count incremented


def test_base_repository_dynamic_filter_engine() -> None:
    """Verifies that BaseRepository dynamic filter engine translates operators into valid clauses."""
    repo: BaseRepository[User, uuid.UUID] = BaseRepository(User, session=MagicMock())
    query: Any = select(User)

    filtered_query: Any = repo._apply_filters(
        query,
        email="test@example.com",
        role__in=["admin", "user"],
        name__ilike="%architect%",
    )

    compiled: str = str(filtered_query.compile(compile_kwargs={"literal_binds": False}))
    assert "users.email =" in compiled
    assert "users.role IN" in compiled
    assert "lower(users.name) LIKE lower(" in compiled or "users.name ILIKE" in compiled


def test_base_repository_dynamic_ordering() -> None:
    """Verifies that BaseRepository correctly parses descending '-' prefix and multi-column order."""
    repo: BaseRepository[User, uuid.UUID] = BaseRepository(User, session=MagicMock())
    query: Any = select(User)

    ordered_query: Any = repo._apply_ordering(query, order_by=["-created_at", "email"])
    compiled: str = str(ordered_query.compile(compile_kwargs={"literal_binds": False}))
    assert "users.created_at DESC" in compiled
    assert "users.email ASC" in compiled


@pytest.mark.asyncio
async def test_base_service_null_cache_polymorphism(mock_user_repo: UserRepository) -> None:
    """Verifies that BaseService operates cleanly with NullCache when no distributed cache is provided."""
    from app.models.product import Product
    from app.repositories.product import ProductRepository
    from app.schemas.product import ProductCreate, ProductRead

    mock_product_repo: MagicMock = MagicMock(spec=ProductRepository)
    mock_product_repo.model = Product

    service: BaseService[Product, ProductCreate, Any, ProductRead, uuid.UUID] = BaseService(
        repo=mock_product_repo, read_schema_cls=ProductRead, cache=None
    )
    assert isinstance(service.cache, NullCache)

    product_id: uuid.UUID = uuid.uuid4()
    dummy_product: Product = Product(
        id=product_id,
        name="NullCache Product",
        sku="NC-001",
        price_cents=1999,
        category="general",
        is_active=True,
        created_at=datetime.now(timezone.utc),
    )
    mock_product_repo.get = AsyncMock(return_value=dummy_product)

    result: ProductRead = await service.get_by_id(product_id)
    assert result.name == "NullCache Product"
    assert mock_product_repo.get.call_count == 1


def test_service_db_session_initialization() -> None:
    """Verifies that services automatically instantiate repositories when passed db: AsyncSession directly."""
    from app.repositories.product import ProductRepository
    from app.services.product import ProductService

    mock_session: AsyncMock = AsyncMock()

    user_service: UserService = UserService(db=mock_session)
    assert isinstance(user_service.repository, UserRepository)
    assert user_service.db is mock_session
    assert user_service.repo.session is mock_session

    product_service: ProductService = ProductService(db=mock_session)
    assert isinstance(product_service.repository, ProductRepository)
    assert product_service.db is mock_session
    assert product_service.repo.session is mock_session


@pytest.mark.asyncio
async def test_user_service_crud_pattern_methods(
    mock_user_repo: UserRepository,
    crypto_service: CryptoService,
) -> None:
    """Verifies user service create, get, get_all, update, and delete methods matching repository pattern."""
    service: UserService = UserService(repo=mock_user_repo, crypto=crypto_service)

    user_id: uuid.UUID = uuid.uuid4()
    aad: bytes = f"users.phone:{user_id}".encode("utf-8")
    enc_phone: str = crypto_service.encrypt("+14155551111", aad=aad)

    dummy_user: User = User(
        id=user_id,
        name="Diana Prince",
        email="diana@example.com",
        role="user",
        phone_encrypted=enc_phone,
        phone_blind_index="blind_diana",
        created_at=datetime.now(timezone.utc),
    )

    # 1. get
    mock_user_repo.get = AsyncMock(return_value=dummy_user)
    mock_user_repo.get_by_id = AsyncMock(return_value=dummy_user)
    found_user: Optional[UserRead] = await service.get(user_id)
    assert found_user is not None
    assert found_user.name == "Diana Prince"

    # 2. get_all
    mock_user_repo.get_all = AsyncMock(return_value=[dummy_user])
    all_users: list[UserRead] = await service.get_all(limit=10, offset=0)
    assert len(all_users) == 1
    assert all_users[0].email == "diana@example.com"

    # 3. update
    from app.schemas.user import UserUpdate
    mock_user_repo.get = AsyncMock(return_value=dummy_user)
    mock_user_repo.update = AsyncMock(return_value=dummy_user)
    updated_user: Optional[UserRead] = await service.update(user_id, UserUpdate(name="Diana Prince-Wayne"))
    assert updated_user is not None

    # 4. delete
    mock_user_repo.delete = AsyncMock()
    mock_user_repo.delete_by_id = AsyncMock(return_value=True)
    deleted: bool = await service.delete(user_id)
    assert deleted is True


@pytest.mark.asyncio
async def test_base_service_after_hooks_fire_after_commit() -> None:
    """Verifies after_create/after_update/after_delete fire exactly once, after the
    transaction commits, with the expected entity (and payload for after_update)."""
    from app.models.product import Product
    from app.repositories.product import ProductRepository
    from app.schemas.product import ProductCreate, ProductRead, ProductUpdate

    events: list[str] = []

    mock_session: AsyncMock = AsyncMock()
    mock_session.commit = AsyncMock(side_effect=lambda: events.append("commit"))

    mock_product_repo: MagicMock = MagicMock(spec=ProductRepository)
    mock_product_repo.model = Product
    mock_product_repo.session = mock_session
    mock_product_repo.create = AsyncMock(side_effect=lambda entity: events.append("repo.create"))

    class TrackedProductService(BaseService[Product, ProductCreate, ProductUpdate, ProductRead, uuid.UUID]):
        def __init__(self) -> None:
            super().__init__(repo=mock_product_repo, read_schema_cls=ProductRead, db=mock_session)
            self.after_create_calls: list[Product] = []
            self.after_update_calls: list[tuple[Product, ProductUpdate]] = []
            self.after_delete_calls: list[Product] = []

        async def after_create(self, entity: Product) -> None:
            events.append("after_create")
            self.after_create_calls.append(entity)

        async def after_update(self, entity: Product, payload: ProductUpdate) -> None:
            events.append("after_update")
            self.after_update_calls.append((entity, payload))

        async def after_delete(self, entity: Product) -> None:
            events.append("after_delete")
            self.after_delete_calls.append(entity)

    service: TrackedProductService = TrackedProductService()

    # --- create() ---
    created: ProductRead = await service.create(
        ProductCreate(name="Hook Widget", sku="HOOK-1", price_cents=500)
    )
    assert len(service.after_create_calls) == 1
    assert service.after_create_calls[0].name == "Hook Widget"
    assert events == ["repo.create", "commit", "after_create"]

    # --- update() ---
    existing: Product = Product(
        id=uuid.uuid4(), name="Hook Widget", sku="HOOK-1", price_cents=500,
        category="general", is_active=True, created_at=datetime.now(timezone.utc),
    )
    events.clear()
    mock_product_repo.get = AsyncMock(return_value=existing)
    mock_product_repo.update = AsyncMock(side_effect=lambda entity: events.append("repo.update"))
    update_payload = ProductUpdate(price_cents=999)
    await service.update(existing.id, update_payload)
    assert len(service.after_update_calls) == 1
    updated_entity, received_payload = service.after_update_calls[0]
    assert updated_entity is existing
    assert received_payload is update_payload
    assert events == ["repo.update", "commit", "after_update"]

    # --- delete() ---
    events.clear()
    mock_product_repo.get = AsyncMock(return_value=existing)
    mock_product_repo.delete = AsyncMock(side_effect=lambda entity: events.append("repo.delete"))
    deleted: bool = await service.delete(existing.id)
    assert deleted is True
    assert len(service.after_delete_calls) == 1
    assert service.after_delete_calls[0] is existing
    assert events == ["repo.delete", "commit", "after_delete"]


