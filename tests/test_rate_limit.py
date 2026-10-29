"""Unit tests for atomic Redis rate limiting and trusted proxy IP resolution."""

from typing import Any, Dict, Optional
from unittest.mock import AsyncMock, MagicMock
from fastapi import HTTPException, Request, Response
import pytest
from redis.exceptions import RedisError

from app.core.config import Settings
from app.services.redis_rate_limit import RateLimiter, resolve_client_ip


def create_mock_request(client_ip: str, headers: Optional[Dict[str, str]] = None) -> Request:
    """Creates a mock FastAPI Request object."""
    mock_app: MagicMock = MagicMock()
    mock_app.state = MagicMock()
    scope: Dict[str, Any] = {
        "type": "http",
        "app": mock_app,
        "client": (client_ip, 54321),
        "headers": [(k.lower().encode("latin-1"), v.encode("latin-1")) for k, v in (headers or {}).items()],
    }
    req: Request = Request(scope)
    return req


def test_ip_resolution_trusted_vs_untrusted() -> None:
    """Verifies X-Forwarded-For is ONLY trusted when request peer is within TRUSTED_PROXIES."""
    settings: Settings = Settings(TRUSTED_PROXIES="127.0.0.1,10.0.0.0/8")

    # Case 1: Direct peer is an untrusted public IP -> Ignore spoofed X-Forwarded-For
    untrusted_req: Request = create_mock_request(
        client_ip="203.0.113.5",
        headers={"X-Forwarded-For": "198.51.100.22"},
    )
    resolved: str = resolve_client_ip(untrusted_req, settings)
    assert resolved == "203.0.113.5"  # Kept direct peer IP!

    # Case 2: Direct peer is a trusted proxy (127.0.0.1) -> Trust X-Forwarded-For
    trusted_req: Request = create_mock_request(
        client_ip="127.0.0.1",
        headers={"X-Forwarded-For": "198.51.100.22, 10.0.0.1"},
    )
    resolved = resolve_client_ip(trusted_req, settings)
    assert resolved == "198.51.100.22"  # Successfully extracted true client IP


@pytest.mark.asyncio
async def test_rate_limiter_under_limit(mock_redis: Any) -> None:
    """Verifies that allowed requests populate rate limit headers correctly."""
    limiter: RateLimiter = RateLimiter(limit=5, window_seconds=60, scope="test")
    request: Request = create_mock_request("192.168.1.50")
    request.app.state.redis = mock_redis
    request.app.state.rate_limit_lua_sha = "mock_sha"
    response: Response = Response()

    await limiter(request=request, response=response, current_user_id=None, settings=Settings())

    assert response.headers["X-RateLimit-Limit"] == "5"
    assert response.headers["X-RateLimit-Remaining"] == "4"
    assert "X-RateLimit-Reset" in response.headers


@pytest.mark.asyncio
async def test_rate_limiter_exceeded_raises_429(mock_redis: Any) -> None:
    """Verifies that exceeding the quota raises HTTP 429 with Retry-After header."""
    limiter: RateLimiter = RateLimiter(limit=2, window_seconds=60, scope="test")
    request: Request = create_mock_request("192.168.1.99")
    request.app.state.redis = mock_redis
    request.app.state.rate_limit_lua_sha = "mock_sha"
    response: Response = Response()

    # Hit 1: allowed
    await limiter(request=request, response=response, current_user_id=None, settings=Settings())
    # Hit 2: allowed
    await limiter(request=request, response=response, current_user_id=None, settings=Settings())

    # Hit 3: exceeded -> 429
    with pytest.raises(HTTPException) as exc_info:
        await limiter(request=request, response=response, current_user_id=None, settings=Settings())

    assert exc_info.value.status_code == 429
    assert "Retry-After" in exc_info.value.headers
    assert exc_info.value.headers["X-RateLimit-Remaining"] == "0"


@pytest.mark.asyncio
async def test_rate_limiter_fail_open_on_redis_error() -> None:
    """Verifies that rate limiter fails open (passes request) when Redis fails and FAIL_OPEN=True."""
    limiter: RateLimiter = RateLimiter(limit=10, window_seconds=60, scope="test")
    broken_redis: AsyncMock = AsyncMock()
    broken_redis.evalsha.side_effect = RedisError("Cluster connection lost")

    request: Request = create_mock_request("10.0.0.1")
    request.app.state.redis = broken_redis
    request.app.state.rate_limit_lua_sha = "sha"
    response: Response = Response()

    settings: Settings = Settings(RATE_LIMIT_FAIL_OPEN=True)
    # Should not raise exception
    await limiter(request=request, response=response, current_user_id=None, settings=settings)

    # When FAIL_OPEN is false, it raises 503
    settings_strict: Settings = Settings(RATE_LIMIT_FAIL_OPEN=False)
    with pytest.raises(HTTPException) as exc_info:
        await limiter(request=request, response=response, current_user_id=None, settings=settings_strict)
    assert exc_info.value.status_code == 503
