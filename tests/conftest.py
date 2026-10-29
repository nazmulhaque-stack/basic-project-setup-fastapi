"""Pytest configuration and shared test fixtures."""

from typing import Any, Dict, List, Optional
import pytest

from app.core.security import CryptoService

# Stable 32-byte test keys (64 hex characters)
TEST_KEY_V1: bytes = bytes.fromhex("1111111111111111111111111111111111111111111111111111111111111111")
TEST_KEY_V2: bytes = bytes.fromhex("2222222222222222222222222222222222222222222222222222222222222222")
TEST_BLIND_KEY: bytes = bytes.fromhex("9999999999999999999999999999999999999999999999999999999999999999")


@pytest.fixture
def test_crypto_service() -> CryptoService:
    """Fixture providing CryptoService configured with multiple versions for key rotation tests."""
    keys: Dict[int, bytes] = {
        1: TEST_KEY_V1,
        2: TEST_KEY_V2,
    }
    return CryptoService(keys=keys, active_version=2, blind_index_key=TEST_BLIND_KEY)


class MockRedisAsync:
    """In-memory async mock simulating Redis Cluster operations for tests."""

    def __init__(self) -> None:
        self.store: Dict[str, bytes | str] = {}
        self.ttls: Dict[str, int] = {}
        self.scripts: Dict[str, str] = {}
        self.rate_counters: Dict[str, int] = {}

    async def get(self, key: str) -> Optional[bytes]:
        val: Optional[bytes | str] = self.store.get(key)
        if isinstance(val, str):
            return val.encode("utf-8")
        return val

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
        count: int = 0
        for k in keys:
            if k in self.store:
                del self.store[k]
                self.ttls.pop(k, None)
                count += 1
        return count

    async def incr(self, key: str) -> int:
        cur: int = int(self.store.get(key, b"1"))
        cur += 1
        self.store[key] = str(cur).encode("utf-8")
        return cur

    async def script_load(self, script: str) -> str:
        sha: str = f"sha_{len(self.scripts) + 1}"
        self.scripts[sha] = script
        return sha

    async def evalsha(self, sha: str, numkeys: int, *args: Any) -> List[int]:
        # Simulates rate limiter Lua script execution
        key: str = str(args[0])
        _limit: int = int(args[1])
        window: int = int(args[2])
        cur: int = self.rate_counters.get(key, 0) + 1
        self.rate_counters[key] = cur
        ttl: int = window
        return [cur, ttl]

    async def eval(self, script: str, numkeys: int, *args: Any) -> List[int]:
        return await self.evalsha("mock", numkeys, *args)

    async def ping(self) -> bool:
        return True

    async def aclose(self) -> None:
        self.store.clear()


@pytest.fixture
def mock_redis() -> MockRedisAsync:
    """Fixture providing isolated MockRedisAsync instance."""
    return MockRedisAsync()
