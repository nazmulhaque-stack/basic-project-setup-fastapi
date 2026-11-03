"""Authentication token manager backed by Redis.

Handles JWT/opaque access tokens, refresh tokens, user sessions,
OTP codes, and password reset tokens — all in dedicated key namespaces
with appropriate TTLs, never stored as ordinary query cache entries.

Key namespaces (never collide with DynamicRedisCache's cache:* namespace):
  auth:access:<token_hash>       -> user_id  (TTL = ACCESS_TOKEN_TTL)
  auth:refresh:<token_hash>      -> {user_id, issued_at}  (TTL = REFRESH_TOKEN_TTL)
  auth:session:<session_id>      -> session payload  (TTL = SESSION_TTL)
  auth:otp:<scope>:<user_id>     -> hashed OTP  (TTL = OTP_TTL)
  auth:reset:<token_hash>        -> user_id  (TTL = RESET_TOKEN_TTL)
  auth:revoked:<token_hash>      -> "1"  (TTL = ACCESS_TOKEN_TTL, for token blacklist)
  auth:user_sessions:<user_id>   -> Set of session_ids (for session enumeration)
"""

from __future__ import annotations

import hashlib
import logging
import time
import uuid
from typing import Any, Dict, Optional, Union

import orjson
from fastapi import Request
from redis.asyncio import Redis, RedisCluster
from redis.exceptions import ConnectionError as RedisConnError, RedisError

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Default TTL constants (seconds)
# ---------------------------------------------------------------------------
_ACCESS_TOKEN_TTL: int = 3_600       # 1 hour
_REFRESH_TOKEN_TTL: int = 2_592_000  # 30 days
_SESSION_TTL: int = 86_400           # 24 hours
_OTP_TTL: int = 300                  # 5 minutes
_RESET_TOKEN_TTL: int = 900          # 15 minutes


def _sha256_hex(value: str) -> str:
    """Return the lowercase SHA-256 hex digest of *value*."""
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


class AuthTokenManager:
    """Redis-backed authentication token manager.

    All public methods are safe to call without wrapping in try/except —
    failures are logged as warnings and safe fallback values are returned.

    The class is intentionally stateless beyond the Redis client reference so
    it is safe to create a fresh instance per-request via the FastAPI
    dependency factory :func:`get_auth_token_manager`.
    """

    def __init__(self, redis: Union[Redis, RedisCluster]) -> None:
        """Initialise the manager with an active asyncio Redis client.

        Args:
            redis: An already-connected :class:`redis.asyncio.Redis` or
                   :class:`redis.asyncio.cluster.RedisCluster` instance.
        """
        self._redis: Union[Redis, RedisCluster] = redis

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _access_key(self, token_hash: str) -> str:
        return f"auth:access:{token_hash}"

    def _revoked_key(self, token_hash: str) -> str:
        return f"auth:revoked:{token_hash}"

    def _refresh_key(self, token_hash: str) -> str:
        return f"auth:refresh:{token_hash}"

    def _session_key(self, session_id: str) -> str:
        return f"auth:session:{session_id}"

    def _user_sessions_key(self, user_id: str) -> str:
        return f"auth:user_sessions:{user_id}"

    def _otp_key(self, scope: str, user_id: str) -> str:
        return f"auth:otp:{scope}:{user_id}"

    def _reset_key(self, token_hash: str) -> str:
        return f"auth:reset:{token_hash}"

    # ------------------------------------------------------------------
    # Access tokens
    # ------------------------------------------------------------------

    async def store_access_token(
        self,
        token: str,
        user_id: str,
        ttl_seconds: int = _ACCESS_TOKEN_TTL,
    ) -> bool:
        """Store an access token mapped to *user_id*.

        The raw token is never persisted; only its SHA-256 hash is used as
        the Redis key suffix.

        Args:
            token:       The opaque or JWT access token string.
            user_id:     Identifier of the authenticated user.
            ttl_seconds: Time-to-live for the Redis entry (default 3 600 s).

        Returns:
            ``True`` on success, ``False`` on Redis failure.
        """
        token_hash = _sha256_hex(token)
        key = self._access_key(token_hash)
        payload = orjson.dumps({"user_id": user_id, "issued_at": time.time()})
        try:
            await self._redis.set(key, payload, ex=ttl_seconds)
            return True
        except (RedisError, RedisConnError, OSError) as exc:
            logger.warning("store_access_token failed for user %s: %s", user_id, exc)
            return False

    async def verify_access_token(self, token: str) -> Optional[str]:
        """Verify an access token and return the associated *user_id*.

        Checks the revocation blacklist before looking up the active token
        store.  Returns ``None`` when the token is expired, revoked, or the
        Redis lookup fails.

        Args:
            token: The raw access token string presented by the client.

        Returns:
            The ``user_id`` string if the token is valid, otherwise ``None``.
        """
        token_hash = _sha256_hex(token)
        try:
            # Revocation check — O(1) per lookup
            revoked = await self._redis.exists(self._revoked_key(token_hash))
            if revoked:
                return None

            raw = await self._redis.get(self._access_key(token_hash))
            if raw is None:
                return None

            data: Dict[str, Any] = orjson.loads(raw)
            return str(data.get("user_id", "")) or None
        except (RedisError, RedisConnError, OSError) as exc:
            logger.warning("verify_access_token lookup error: %s", exc)
            return None

    async def revoke_access_token(
        self,
        token: str,
        ttl_seconds: int = _ACCESS_TOKEN_TTL,
    ) -> bool:
        """Add token to the revocation blacklist and remove its active entry.

        The revocation key inherits the same TTL as the original access token
        so the blacklist entry expires naturally once the token would have
        been invalid regardless.

        Args:
            token:       The raw access token to revoke.
            ttl_seconds: TTL for the blacklist entry (should match issuance TTL).

        Returns:
            ``True`` on success, ``False`` on Redis failure.
        """
        token_hash = _sha256_hex(token)
        try:
            pipe = self._redis.pipeline(transaction=False)  # type: ignore[union-attr]
            pipe.set(self._revoked_key(token_hash), b"1", ex=ttl_seconds)
            pipe.delete(self._access_key(token_hash))
            await pipe.execute()
            return True
        except (RedisError, RedisConnError, OSError) as exc:
            logger.warning("revoke_access_token failed: %s", exc)
            return False

    # ------------------------------------------------------------------
    # Refresh tokens
    # ------------------------------------------------------------------

    async def store_refresh_token(
        self,
        token: str,
        user_id: str,
        ttl_seconds: int = _REFRESH_TOKEN_TTL,
    ) -> bool:
        """Persist a refresh token entry in Redis.

        Args:
            token:       The opaque refresh token string.
            user_id:     Identifier of the authenticated user.
            ttl_seconds: Time-to-live (default 30 days).

        Returns:
            ``True`` on success, ``False`` on Redis failure.
        """
        token_hash = _sha256_hex(token)
        key = self._refresh_key(token_hash)
        payload = orjson.dumps({"user_id": user_id, "issued_at": time.time()})
        try:
            await self._redis.set(key, payload, ex=ttl_seconds)
            return True
        except (RedisError, RedisConnError, OSError) as exc:
            logger.warning("store_refresh_token failed for user %s: %s", user_id, exc)
            return False

    async def verify_refresh_token(self, token: str) -> Optional[str]:
        """Verify a refresh token and return the associated *user_id*.

        Args:
            token: The raw refresh token string.

        Returns:
            The ``user_id`` string if valid, otherwise ``None``.
        """
        token_hash = _sha256_hex(token)
        try:
            raw = await self._redis.get(self._refresh_key(token_hash))
            if raw is None:
                return None
            data: Dict[str, Any] = orjson.loads(raw)
            return str(data.get("user_id", "")) or None
        except (RedisError, RedisConnError, OSError) as exc:
            logger.warning("verify_refresh_token lookup error: %s", exc)
            return None

    async def revoke_refresh_token(self, token: str) -> bool:
        """Delete the refresh token entry from Redis.

        Args:
            token: The raw refresh token to revoke.

        Returns:
            ``True`` on success (including key not found), ``False`` on Redis
            failure.
        """
        token_hash = _sha256_hex(token)
        try:
            await self._redis.delete(self._refresh_key(token_hash))
            return True
        except (RedisError, RedisConnError, OSError) as exc:
            logger.warning("revoke_refresh_token failed: %s", exc)
            return False

    # ------------------------------------------------------------------
    # Sessions
    # ------------------------------------------------------------------

    async def create_session(
        self,
        user_id: str,
        session_data: Dict[str, Any],
        ttl_seconds: int = _SESSION_TTL,
    ) -> str:
        """Create a new user session and persist it in Redis.

        A unique UUID session identifier is generated and stored under
        ``auth:session:<session_id>``.  The session ID is also added to the
        ``auth:user_sessions:<user_id>`` Redis Set so that all active
        sessions for a user can be enumerated and bulk-invalidated.

        Args:
            user_id:      The owner of the session.
            session_data: Arbitrary JSON-serialisable payload to store.
            ttl_seconds:  Time-to-live for the session entry (default 24 h).

        Returns:
            The newly created ``session_id`` string.  Returns an empty string
            on Redis failure (caller should treat this as a degraded state).
        """
        session_id = str(uuid.uuid4())
        session_key = self._session_key(session_id)
        user_set_key = self._user_sessions_key(user_id)
        payload = orjson.dumps({"user_id": user_id, "session_id": session_id, **session_data})
        try:
            pipe = self._redis.pipeline(transaction=False)  # type: ignore[union-attr]
            pipe.set(session_key, payload, ex=ttl_seconds)
            pipe.sadd(user_set_key, session_id)
            # Keep the user-sessions set alive as long as the longest session
            pipe.expire(user_set_key, ttl_seconds)
            await pipe.execute()
            return session_id
        except (RedisError, RedisConnError, OSError) as exc:
            logger.warning("create_session failed for user %s: %s", user_id, exc)
            return ""

    async def get_session(self, session_id: str) -> Optional[Dict[str, Any]]:
        """Retrieve session data by *session_id*.

        Args:
            session_id: The UUID session identifier.

        Returns:
            The deserialised session dict, or ``None`` if not found / expired.
        """
        try:
            raw = await self._redis.get(self._session_key(session_id))
            if raw is None:
                return None
            return orjson.loads(raw)  # type: ignore[return-value]
        except (RedisError, RedisConnError, OSError) as exc:
            logger.warning("get_session failed for session %s: %s", session_id, exc)
            return None

    async def invalidate_session(self, session_id: str) -> bool:
        """Delete a single session entry from Redis.

        Args:
            session_id: The UUID session identifier to invalidate.

        Returns:
            ``True`` on success, ``False`` on Redis failure.
        """
        try:
            await self._redis.delete(self._session_key(session_id))
            return True
        except (RedisError, RedisConnError, OSError) as exc:
            logger.warning("invalidate_session failed for session %s: %s", session_id, exc)
            return False

    async def invalidate_all_user_sessions(self, user_id: str) -> int:
        """Invalidate every active session belonging to *user_id*.

        Reads the ``auth:user_sessions:<user_id>`` Redis Set, deletes each
        ``auth:session:<id>`` key, and then removes the set itself.

        Args:
            user_id: The user whose sessions should all be revoked.

        Returns:
            The number of session keys that were deleted.  Returns ``0`` on
            Redis failure.
        """
        user_set_key = self._user_sessions_key(user_id)
        try:
            session_ids = await self._redis.smembers(user_set_key)
            if not session_ids:
                await self._redis.delete(user_set_key)
                return 0

            session_keys = [self._session_key(sid.decode("utf-8") if isinstance(sid, bytes) else sid) for sid in session_ids]
            pipe = self._redis.pipeline(transaction=False)  # type: ignore[union-attr]
            for key in session_keys:
                pipe.delete(key)
            pipe.delete(user_set_key)
            await pipe.execute()
            return len(session_ids)
        except (RedisError, RedisConnError, OSError) as exc:
            logger.warning("invalidate_all_user_sessions failed for user %s: %s", user_id, exc)
            return 0

    # ------------------------------------------------------------------
    # OTP (one-time passwords)
    # ------------------------------------------------------------------

    async def store_otp(
        self,
        scope: str,
        user_id: str,
        otp: str,
        ttl_seconds: int = _OTP_TTL,
    ) -> bool:
        """Store a hashed OTP for *user_id* under the given *scope*.

        Only the SHA-256 digest of the plaintext OTP is persisted so that a
        Redis compromise does not expose raw codes.

        Args:
            scope:       A logical grouping for the OTP (e.g. ``"phone_verify"``).
            user_id:     The user the OTP is issued to.
            otp:         The plaintext OTP code (4–8 digits, etc.).
            ttl_seconds: Time-to-live (default 5 min).

        Returns:
            ``True`` on success, ``False`` on Redis failure.
        """
        otp_hash = hashlib.sha256(otp.encode("utf-8")).hexdigest()
        key = self._otp_key(scope, user_id)
        try:
            await self._redis.set(key, otp_hash.encode("utf-8"), ex=ttl_seconds)
            return True
        except (RedisError, RedisConnError, OSError) as exc:
            logger.warning("store_otp failed for user %s scope %s: %s", user_id, scope, exc)
            return False

    async def verify_otp(
        self,
        scope: str,
        user_id: str,
        otp: str,
        consume: bool = True,
    ) -> bool:
        """Verify an OTP code for *user_id* under *scope*.

        Compares the SHA-256 digest of the supplied *otp* with the stored
        digest.  If *consume* is ``True`` (the default) the OTP is deleted
        from Redis after a successful match, making it single-use.

        Args:
            scope:   The logical OTP scope used when the OTP was stored.
            user_id: The user the OTP was issued to.
            otp:     The plaintext OTP code to verify.
            consume: When ``True``, delete the OTP key after successful
                     verification so it cannot be reused.

        Returns:
            ``True`` if the OTP matches, ``False`` otherwise (including Redis
            failures).
        """
        key = self._otp_key(scope, user_id)
        expected_hash = hashlib.sha256(otp.encode("utf-8")).hexdigest()
        try:
            raw = await self._redis.get(key)
            if raw is None:
                return False
            stored_hash = raw.decode("utf-8") if isinstance(raw, bytes) else raw
            if stored_hash != expected_hash:
                return False
            if consume:
                await self._redis.delete(key)
            return True
        except (RedisError, RedisConnError, OSError) as exc:
            logger.warning("verify_otp failed for user %s scope %s: %s", user_id, scope, exc)
            return False

    # ------------------------------------------------------------------
    # Password reset tokens
    # ------------------------------------------------------------------

    async def store_reset_token(
        self,
        token: str,
        user_id: str,
        ttl_seconds: int = _RESET_TOKEN_TTL,
    ) -> bool:
        """Store a password-reset token mapped to *user_id*.

        Args:
            token:       The opaque password-reset token (should be a
                         cryptographically random string, e.g. 32 bytes hex).
            user_id:     The user requesting the password reset.
            ttl_seconds: Time-to-live (default 15 min).

        Returns:
            ``True`` on success, ``False`` on Redis failure.
        """
        token_hash = _sha256_hex(token)
        key = self._reset_key(token_hash)
        payload = orjson.dumps({"user_id": user_id, "issued_at": time.time()})
        try:
            await self._redis.set(key, payload, ex=ttl_seconds)
            return True
        except (RedisError, RedisConnError, OSError) as exc:
            logger.warning("store_reset_token failed for user %s: %s", user_id, exc)
            return False

    async def verify_reset_token(self, token: str) -> Optional[str]:
        """Verify and consume a password-reset token.

        Reset tokens are **always** consumed on the first successful
        verification to prevent reuse.

        Args:
            token: The raw password-reset token string.

        Returns:
            The ``user_id`` string if the token is valid, otherwise ``None``.
        """
        token_hash = _sha256_hex(token)
        key = self._reset_key(token_hash)
        try:
            raw = await self._redis.get(key)
            if raw is None:
                return None
            data: Dict[str, Any] = orjson.loads(raw)
            user_id = str(data.get("user_id", "")) or None
            if user_id:
                # Consume immediately — single-use token
                await self._redis.delete(key)
            return user_id
        except (RedisError, RedisConnError, OSError) as exc:
            logger.warning("verify_reset_token lookup error: %s", exc)
            return None


# ---------------------------------------------------------------------------
# FastAPI dependency factory
# ---------------------------------------------------------------------------


def get_auth_token_manager(request: Request) -> AuthTokenManager:
    """FastAPI dependency that resolves an :class:`AuthTokenManager` from the
    application state Redis client.

    Usage::

        @router.post("/login")
        async def login(
            manager: Annotated[AuthTokenManager, Depends(get_auth_token_manager)],
        ) -> ...:
            ...

    Args:
        request: The incoming FastAPI :class:`~fastapi.Request` object.

    Returns:
        A ready-to-use :class:`AuthTokenManager` instance.
    """
    return AuthTokenManager(request.app.state.redis)


__all__ = [
    "AuthTokenManager",
    "get_auth_token_manager",
]
