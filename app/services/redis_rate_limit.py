"""High-performance Redis Cluster Rate Limiter using atomic Lua script and EVALSHA.

Architecture & Security Notes:
-----------------------------
1. DDoS Defense Layering:
   - Primary volumetric DDoS defense (SYN floods, HTTP request floods) MUST live in Nginx
     and upstream firewalls/Cloudflare (using `limit_req` and `limit_conn` zones).
   - This application-level rate limiter protects backend business logic and databases from
     per-user and per-IP abuse and noisy-neighbor saturation.
2. Atomicity & No-Pipeline Principle:
   - Plain pipelines send multiple commands in batch, but do NOT execute atomically. A failure
     or restart between INCR and EXPIRE leaves the key without an expiry (memory leak).
   - Extending EXPIRE on every request penalizes abusive clients incorrectly (their window
     keeps resetting into the future). We set EXPIRE strictly when current == 1.
3. Cluster Slot Pinning:
   - Keys use hash tags: `rl:{<scope>:<identity>}:<window_start>`. The `{...}` forces Redis
     Cluster to route the key to a single determined hash slot.
4. Proxy Trust:
   - X-Forwarded-For is ONLY trusted if the direct peer IP falls within TRUSTED_PROXIES CIDRs.
     This prevents IP spoofing attacks where clients forge arbitrary client IPs.
5. Fail-Open Architecture:
   - If Redis Cluster is temporarily unreachable, logs a warning and allows requests through
     (when RATE_LIMIT_FAIL_OPEN=true), ensuring database availability does not bring down the API.
"""

import ipaddress
import logging
import time
from typing import Any, Optional, Union

from fastapi import Depends, HTTPException, Request, Response, status
from redis.asyncio import Redis, RedisCluster
from redis.exceptions import NoScriptError, RedisError

from app.core.config import Settings, get_settings

logger = logging.getLogger(__name__)

# Atomic Lua script: INCR counter, set EXPIRE only on initial creation (current == 1)
RATE_LIMIT_LUA_SCRIPT = """
local key = KEYS[1]
local limit = tonumber(ARGV[1])
local window = tonumber(ARGV[2])

local current = redis.call('INCR', key)
if current == 1 then
    redis.call('EXPIRE', key, window)
end
local ttl = redis.call('TTL', key)
if ttl == -1 then
    redis.call('EXPIRE', key, window)
    ttl = window
end

return {current, ttl}
"""


async def get_current_user_optional(request: Request) -> Optional[str]:
    """Stub dependency for JWT authentication.

    Extracts user ID from Authorization Bearer token 'sub' claim if present.
    In production, this decodes and verifies RS256/EdDSA JWT signature.
    """
    auth_header: Optional[str] = request.headers.get("authorization")
    if not auth_header or not auth_header.startswith("Bearer "):
        return None

    token: str = auth_header.split(" ", 1)[1].strip()
    if not token:
        return None

    # In a full JWT implementation:
    # payload = jwt.decode(token, public_key, algorithms=["RS256"])
    # return payload.get("sub")
    # For this architecture, we support token simulation or simple user ID extraction:
    if token.startswith("user_"):
        return token
    return None


def resolve_client_ip(request: Request, settings: Settings) -> str:
    """Extracts client IP, respecting X-Forwarded-For ONLY when request peer is a trusted proxy.

    Prevents IP spoofing while correctly identifying remote clients behind Nginx.
    """
    peer_ip_str: str = request.client.host if request.client else "127.0.0.1"

    try:
        peer_ip: Union[ipaddress.IPv4Address, ipaddress.IPv6Address] = ipaddress.ip_address(peer_ip_str)
    except ValueError:
        return peer_ip_str

    # Verify if direct peer belongs to any configured trusted proxy subnet
    is_trusted: bool = any(peer_ip in network for network in settings.parsed_trusted_proxies)

    if is_trusted:
        forwarded_for: Optional[str] = request.headers.get("x-forwarded-for")
        if forwarded_for:
            # Client IP is the leftmost entry in X-Forwarded-For
            client_candidate: str = forwarded_for.split(",")[0].strip()
            try:
                ipaddress.ip_address(client_candidate)
                return client_candidate
            except ValueError:
                pass

        real_ip: Optional[str] = request.headers.get("x-real-ip")
        if real_ip:
            real_ip = real_ip.strip()
            try:
                ipaddress.ip_address(real_ip)
                return real_ip
            except ValueError:
                pass

    return peer_ip_str


class RateLimiter:
    """Callable async dependency enforcing rate limits via Redis EVALSHA."""

    def __init__(self, limit: int, window_seconds: int, scope: str) -> None:
        self.limit: int = limit
        self.window_seconds: int = window_seconds
        self.scope: str = scope

    async def __call__(
        self,
        request: Request,
        response: Response,
        current_user_id: Optional[str] = Depends(get_current_user_optional),
        settings: Settings = Depends(get_settings),
    ) -> None:
        # Resolve identity: authenticated user ID takes precedence, falling back to IP
        identity: str
        if current_user_id:
            identity = f"usr:{current_user_id}"
        else:
            client_ip: str = resolve_client_ip(request, settings)
            identity = f"ip:{client_ip}"

        # Align to fixed window start timestamp to group hits cleanly
        now: int = int(time.time())
        window_start: int = (now // self.window_seconds) * self.window_seconds

        # Redis Cluster hash tag {scope:identity} guarantees routing to single slot
        cache_key: str = f"rl:{{{self.scope}:{identity}}}:{window_start}"

        redis_client: Union[RedisCluster, Redis] = request.app.state.redis
        lua_sha: str = request.app.state.rate_limit_lua_sha

        try:
            result: Any
            try:
                # Attempt fast EVALSHA first (no script compilation overhead)
                result = await redis_client.evalsha(
                    lua_sha,
                    1,
                    cache_key,
                    self.limit,
                    self.window_seconds,
                )
            except NoScriptError:
                # Fallback to direct EVAL if script was flushed from Redis script cache
                result = await redis_client.eval(
                    RATE_LIMIT_LUA_SCRIPT,
                    1,
                    cache_key,
                    self.limit,
                    self.window_seconds,
                )

            current_count: int = int(result[0])
            ttl: int = int(result[1])
            effective_ttl: int = max(ttl, 1)

            if current_count > self.limit:
                logger.warning(
                    "Rate limit exceeded for %s on scope %s: %d/%d (TTL %ds)",
                    identity,
                    self.scope,
                    current_count,
                    self.limit,
                    effective_ttl,
                )
                raise HTTPException(
                    status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                    detail="Rate limit exceeded. Please retry later.",
                    headers={
                        "Retry-After": str(effective_ttl),
                        "X-RateLimit-Limit": str(self.limit),
                        "X-RateLimit-Remaining": "0",
                        "X-RateLimit-Reset": str(effective_ttl),
                    },
                )

            # Request is within quota - populate informative rate limit headers
            remaining: int = max(0, self.limit - current_count)
            response.headers["X-RateLimit-Limit"] = str(self.limit)
            response.headers["X-RateLimit-Remaining"] = str(remaining)
            response.headers["X-RateLimit-Reset"] = str(effective_ttl)

        except HTTPException:
            raise
        except (RedisError, ConnectionError, OSError) as exc:
            if settings.RATE_LIMIT_FAIL_OPEN:
                logger.warning(
                    "Redis rate limiter connection error (%s). Failing OPEN to preserve availability.",
                    exc,
                )
                return
            else:
                logger.error("Redis rate limiter down and FAIL_OPEN is false: %s", exc)
                raise HTTPException(
                    status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                    detail="Rate limiting service temporarily unavailable.",
                )


def rate_limiter(
    limit: int = 10,
    window_seconds: int = 60,
    scope: str = "default",
) -> RateLimiter:
    """Dependency factory generating a configured RateLimiter instance."""
    return RateLimiter(limit=limit, window_seconds=window_seconds, scope=scope)
