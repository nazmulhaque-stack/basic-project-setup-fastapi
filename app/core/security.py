"""Stateless AES-256-GCM encryption service with key rotation and HMAC blind indexing.

Meets the following security requirements:
1. AES-256-GCM authenticated encryption with 32-byte keys.
2. Cryptographically secure 12-byte random nonces generated via os.urandom per operation.
3. Packed format: key_version (1 byte) || nonce (12 bytes) || ciphertext+tag.
4. Output encoded as URL-safe Base64 with padding stripped.
5. Key rotation: Key versions 1..255 mapped to ciphers; encrypt uses active key,
   decrypt dynamically resolves cipher by the header version byte.
6. Associated Data (AAD) binding to thwart ciphertext portability attacks.
7. Blind indexing using HMAC-SHA256 for deterministic, searchable UNIQUE indexes
   without exposing plaintext PII.
8. DecryptionError raised on InvalidTag or tampering without leaking stack traces.
"""

import base64
import hashlib
import hmac
import os
import re
from typing import Dict, Optional, Union

from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from fastapi import Request

from app.core.exceptions import DecryptionError


class CryptoService:
    """Production-grade AES-256-GCM encryption and HMAC blind indexing service."""

    def __init__(self, keys: Dict[int, bytes], active_version: int, blind_index_key: bytes) -> None:
        """Initializes the crypto service with pre-allocated AESGCM cipher instances.

        Args:
            keys: Dictionary mapping integer versions (1-255) to 32-byte secret keys.
            active_version: The key version to be used for all new encryption operations.
            blind_index_key: 32-byte secret key dedicated exclusively to HMAC blind indexing.
        """
        if not keys:
            raise ValueError("CryptoService requires at least one encryption key")
        if active_version not in keys:
            raise ValueError(f"Active key version {active_version} is missing from provided keys")
        if len(blind_index_key) != 32:
            raise ValueError("Blind index key must be exactly 32 bytes")

        self._active_version = active_version
        self._blind_index_key = blind_index_key
        # Pre-allocate stateless AESGCM ciphers for fast multi-threaded execution
        self._ciphers: Dict[int, AESGCM] = {}
        for ver, key_bytes in keys.items():
            if not (1 <= ver <= 255):
                raise ValueError(f"Key version {ver} out of bounds: must fit in 1 byte (1..255)")
            if len(key_bytes) != 32:
                raise ValueError(f"Key version {ver} is not 32 bytes (AES-256 requirement)")
            self._ciphers[ver] = AESGCM(key_bytes)

    @property
    def active_version(self) -> int:
        return self._active_version

    def encrypt(self, plaintext: Union[str, bytes], aad: bytes | None = None) -> str:
        """Encrypts plaintext with AES-256-GCM using active key version and a fresh nonce.

        Output structure: URL-safe Base64 without padding of:
        [1-byte key_version] + [12-byte nonce] + [ciphertext + 16-byte authentication tag]
        """
        data_bytes: bytes = plaintext.encode("utf-8") if isinstance(plaintext, str) else plaintext
        # Fresh 12-byte (96-bit) nonce per NIST SP 800-38D recommendations
        nonce: bytes = os.urandom(12)
        cipher: AESGCM = self._ciphers[self._active_version]

        try:
            ciphertext_and_tag: bytes = cipher.encrypt(nonce, data_bytes, aad)
        except Exception as e:
            raise DecryptionError(f"Encryption failed unexpectedly: {e}") from e

        # Pack key_version (1 byte) + nonce (12 bytes) + ciphertext_and_tag
        header_byte: bytes = bytes([self._active_version])
        raw_payload: bytes = header_byte + nonce + ciphertext_and_tag

        # URL-safe Base64 without padding
        return base64.urlsafe_b64encode(raw_payload).decode("ascii").rstrip("=")

    def decrypt(self, encoded_token: str, aad: bytes | None = None) -> str:
        """Decrypts URL-safe Base64 token with key version lookup and tag verification.

        Args:
            encoded_token: URL-safe base64 string containing version, nonce, and ciphertext+tag.
            aad: Associated authenticated data. Must match the exact AAD used during encryption.

        Returns:
            Decrypted plaintext string.

        Raises:
            DecryptionError: If the token is invalid, tampered with, wrong key version, or corrupted.
        """
        if not encoded_token:
            raise DecryptionError("Ciphertext token cannot be empty")

        # Restore URL-safe base64 padding if needed
        clean_token: str = encoded_token.strip()
        missing_padding: int = len(clean_token) % 4
        if missing_padding:
            clean_token += "=" * (4 - missing_padding)

        try:
            raw_payload: bytes = base64.urlsafe_b64decode(clean_token.encode("ascii"))
        except Exception as e:
            raise DecryptionError("Invalid Base64 encoding in ciphertext token") from e

        # Minimum length: 1 byte (version) + 12 bytes (nonce) + 16 bytes (AES-GCM tag) = 29 bytes
        if len(raw_payload) < 29:
            raise DecryptionError("Ciphertext payload is too short to be valid AES-256-GCM")

        version_byte: int = raw_payload[0]
        nonce: bytes = raw_payload[1:13]
        ciphertext_and_tag: bytes = raw_payload[13:]

        cipher: Optional[AESGCM] = self._ciphers.get(version_byte)
        if cipher is None:
            raise DecryptionError(
                f"Unknown or rotated-out key version: {version_byte}. Available versions: {list(self._ciphers.keys())}"
            )

        try:
            plaintext_bytes: bytes = cipher.decrypt(nonce, ciphertext_and_tag, aad)
            return plaintext_bytes.decode("utf-8")
        except InvalidTag as e:
            # Constant-time comparison failure in AESGCM tag verification
            raise DecryptionError("Decryption failed: authentication tag mismatch or corrupted data/AAD") from e
        except Exception as e:
            raise DecryptionError(f"Decryption failed: {e}") from e

    def blind_index(self, value: str) -> str:
        """Computes a deterministic, non-reversible HMAC-SHA256 hash for exact-match querying.

        The value is normalized first (e.g., stripping non-digit characters except leading '+').
        """
        normalized: str = self.normalize_phone(value)
        mac: hmac.HMAC = hmac.new(self._blind_index_key, normalized.encode("utf-8"), hashlib.sha256)
        return mac.hexdigest()

    @staticmethod
    def normalize_phone(phone: str) -> str:
        """Normalizes a phone number to standard E.164-like representation.

        Strips spaces, dashes, parentheses, dots. Ensures leading '+'.
        """
        cleaned: str = phone.strip()
        has_plus: bool = cleaned.startswith("+")
        digits_only: str = re.sub(r"\D", "", cleaned)
        if not digits_only:
            raise ValueError("Phone number must contain at least one digit")
        return f"+{digits_only}" if has_plus else f"+{digits_only}"

    @staticmethod
    def mask_phone(phone: str) -> str:
        """Masks a phone number for safe external display without leaking PII.

        Example: '+12025550198' -> '+1******0198'
        """
        cleaned: str = phone.strip()
        if len(cleaned) <= 6:
            return "***"
        prefix: str = cleaned[:2]
        suffix: str = cleaned[-4:]
        mask_length: int = max(len(cleaned) - 6, 4)
        return f"{prefix}{'*' * mask_length}{suffix}"


def get_crypto_service(request: Request) -> CryptoService:
    """FastAPI dependency yielding the singleton CryptoService instance stored on app.state."""
    return request.app.state.crypto_service


__all__ = ["CryptoService", "get_crypto_service"]

