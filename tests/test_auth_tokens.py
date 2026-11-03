"""Unit tests for AuthTokenManager (Redis-backed tokens, sessions, OTP, reset tokens)."""

import hashlib
from typing import Any, Dict, Optional, Set
import orjson
import pytest

from app.core.auth_tokens import AuthTokenManager


class MockRedisForAuth:
    """Mock Redis client simulating key-value, pipeline, and Set operations for AuthTokenManager."""

    def __init__(self) -> None:
        self.store: Dict[str, bytes] = {}
        self.sets: Dict[str, Set[str]] = {}
        self.ttls: Dict[str, int] = {}

    async def get(self, key: str) -> Optional[bytes]:
        return self.store.get(key)

    async def set(
        self,
        key: str,
        value: Any,
        ex: Optional[int] = None,
        px: Optional[int] = None,
        nx: bool = False,
    ) -> bool:
        if nx and key in self.store:
            return False
        self.store[key] = value if isinstance(value, (bytes, bytearray)) else str(value).encode("utf-8")
        if ex:
            self.ttls[key] = ex
        return True

    async def delete(self, *keys: str) -> int:
        count = 0
        for k in keys:
            if k in self.store:
                del self.store[k]
                self.ttls.pop(k, None)
                count += 1
            if k in self.sets:
                del self.sets[k]
                count += 1
        return count

    async def exists(self, *keys: str) -> int:
        return sum(1 for k in keys if k in self.store or k in self.sets)

    async def smembers(self, key: str) -> Set[str]:
        return self.sets.get(key, set()).copy()

    def pipeline(self, transaction: bool = False) -> "MockPipeline":
        return MockPipeline(self)


class MockPipeline:
    """Mock pipeline that records operations and executes them against MockRedisForAuth."""

    def __init__(self, redis: MockRedisForAuth) -> None:
        self.redis = redis
        self.ops = []

    def set(self, key: str, value: Any, ex: Optional[int] = None) -> "MockPipeline":
        self.ops.append(("set", key, value, ex))
        return self

    def delete(self, key: str) -> "MockPipeline":
        self.ops.append(("delete", key))
        return self

    def sadd(self, key: str, *members: str) -> "MockPipeline":
        self.ops.append(("sadd", key, members))
        return self

    def expire(self, key: str, ttl: int) -> "MockPipeline":
        self.ops.append(("expire", key, ttl))
        return self

    async def execute(self) -> list:
        results = []
        for op in self.ops:
            if op[0] == "set":
                _, key, val, ex = op
                await self.redis.set(key, val, ex=ex)
                results.append(True)
            elif op[0] == "delete":
                _, key = op
                await self.redis.delete(key)
                results.append(1)
            elif op[0] == "sadd":
                _, key, members = op
                if key not in self.redis.sets:
                    self.redis.sets[key] = set()
                for m in members:
                    self.redis.sets[key].add(m)
                results.append(len(members))
            elif op[0] == "expire":
                _, key, ttl = op
                self.redis.ttls[key] = ttl
                results.append(True)
        self.ops.clear()
        return results


@pytest.fixture
def auth_redis() -> MockRedisForAuth:
    return MockRedisForAuth()


@pytest.fixture
def auth_mgr(auth_redis: MockRedisForAuth) -> AuthTokenManager:
    return AuthTokenManager(auth_redis)


@pytest.mark.asyncio
async def test_access_token_lifecycle(auth_mgr: AuthTokenManager) -> None:
    token = "secret-access-token-12345"
    user_id = "user-uuid-99"

    # Store
    stored = await auth_mgr.store_access_token(token, user_id, ttl_seconds=3600)
    assert stored is True

    # Verify
    verified_user = await auth_mgr.verify_access_token(token)
    assert verified_user == user_id

    # Verify non-existent
    assert await auth_mgr.verify_access_token("wrong-token") is None

    # Revoke
    revoked = await auth_mgr.revoke_access_token(token)
    assert revoked is True

    # After revocation, verify must return None
    assert await auth_mgr.verify_access_token(token) is None


@pytest.mark.asyncio
async def test_refresh_token_lifecycle(auth_mgr: AuthTokenManager) -> None:
    refresh_token = "refresh-token-abcde"
    user_id = "user-uuid-42"

    stored = await auth_mgr.store_refresh_token(refresh_token, user_id)
    assert stored is True

    verified = await auth_mgr.verify_refresh_token(refresh_token)
    assert verified == user_id

    revoked = await auth_mgr.revoke_refresh_token(refresh_token)
    assert revoked is True

    assert await auth_mgr.verify_refresh_token(refresh_token) is None


@pytest.mark.asyncio
async def test_session_lifecycle(auth_mgr: AuthTokenManager) -> None:
    user_id = "user-sess-1"
    session_data = {"role": "admin", "ip": "192.168.1.1"}

    session_id = await auth_mgr.create_session(user_id, session_data)
    assert session_id != ""

    fetched = await auth_mgr.get_session(session_id)
    assert fetched is not None
    assert fetched["user_id"] == user_id
    assert fetched["role"] == "admin"

    # Invalidate single session
    ok = await auth_mgr.invalidate_session(session_id)
    assert ok is True
    assert await auth_mgr.get_session(session_id) is None


@pytest.mark.asyncio
async def test_invalidate_all_user_sessions(auth_mgr: AuthTokenManager) -> None:
    user_id = "user-multi-session"

    s1 = await auth_mgr.create_session(user_id, {"device": "mobile"})
    s2 = await auth_mgr.create_session(user_id, {"device": "desktop"})
    s3 = await auth_mgr.create_session(user_id, {"device": "tablet"})

    assert s1 and s2 and s3

    count = await auth_mgr.invalidate_all_user_sessions(user_id)
    assert count == 3

    assert await auth_mgr.get_session(s1) is None
    assert await auth_mgr.get_session(s2) is None
    assert await auth_mgr.get_session(s3) is None


@pytest.mark.asyncio
async def test_otp_lifecycle(auth_mgr: AuthTokenManager) -> None:
    scope = "password_reset"
    user_id = "user-otp-1"
    otp = "482910"

    stored = await auth_mgr.store_otp(scope, user_id, otp, ttl_seconds=300)
    assert stored is True

    # Wrong OTP
    assert await auth_mgr.verify_otp(scope, user_id, "000000") is False

    # Correct OTP with consume=True (default)
    assert await auth_mgr.verify_otp(scope, user_id, otp, consume=True) is True

    # Re-verify must fail because it was consumed
    assert await auth_mgr.verify_otp(scope, user_id, otp) is False


@pytest.mark.asyncio
async def test_reset_token_lifecycle(auth_mgr: AuthTokenManager) -> None:
    token = "secure-random-reset-token-xyz"
    user_id = "user-reset-target"

    stored = await auth_mgr.store_reset_token(token, user_id, ttl_seconds=900)
    assert stored is True

    # Verify and consume
    verified = await auth_mgr.verify_reset_token(token)
    assert verified == user_id

    # Reset tokens are single-use; subsequent check must fail
    assert await auth_mgr.verify_reset_token(token) is None

