"""Unit tests for AES-256-GCM encryption, key rotation, AAD, and HMAC blind indexing."""

import base64
import pytest
from app.core.exceptions import DecryptionError
from app.core.security import CryptoService

TEST_KEY_V1: bytes = bytes.fromhex("1111111111111111111111111111111111111111111111111111111111111111")
TEST_KEY_V2: bytes = bytes.fromhex("2222222222222222222222222222222222222222222222222222222222222222")
TEST_BLIND_KEY: bytes = bytes.fromhex("9999999999999999999999999999999999999999999999999999999999999999")


def test_encryption_roundtrip() -> None:
    """Validates successful AES-256-GCM encryption and decryption roundtrip."""
    crypto: CryptoService = CryptoService(keys={1: TEST_KEY_V1}, active_version=1, blind_index_key=TEST_BLIND_KEY)
    plaintext: str = "+14155552671"
    aad: bytes = b"users.phone:12345"

    token: str = crypto.encrypt(plaintext, aad=aad)
    assert isinstance(token, str)
    # Output must be URL-safe Base64 without padding
    assert "=" not in token

    decrypted: str = crypto.decrypt(token, aad=aad)
    assert decrypted == plaintext


def test_fresh_nonce_per_encryption() -> None:
    """Ensures each encryption generates a unique random nonce even for identical plaintexts."""
    crypto: CryptoService = CryptoService(keys={1: TEST_KEY_V1}, active_version=1, blind_index_key=TEST_BLIND_KEY)
    plaintext: str = "+14155552671"
    aad: bytes = b"users.phone:test"

    token1: str = crypto.encrypt(plaintext, aad=aad)
    token2: str = crypto.encrypt(plaintext, aad=aad)

    assert token1 != token2  # Nonces must differ, yielding distinct ciphertexts


def test_aad_binding_prevents_ciphertext_transplantation() -> None:
    """Verifies that ciphertext copied to another user_id or field fails authentication tag check."""
    crypto: CryptoService = CryptoService(keys={1: TEST_KEY_V1}, active_version=1, blind_index_key=TEST_BLIND_KEY)
    plaintext: str = "+14155552671"
    user_a_aad: bytes = b"users.phone:user-uuid-A"
    user_b_aad: bytes = b"users.phone:user-uuid-B"

    token: str = crypto.encrypt(plaintext, aad=user_a_aad)

    # Decrypting with correct AAD succeeds
    assert crypto.decrypt(token, aad=user_a_aad) == plaintext

    # Decrypting with wrong AAD (e.g. another row) fails with DecryptionError
    with pytest.raises(DecryptionError, match="authentication tag mismatch"):
        crypto.decrypt(token, aad=user_b_aad)

    # Decrypting with None AAD also fails
    with pytest.raises(DecryptionError):
        crypto.decrypt(token, aad=None)


def test_key_rotation_multi_version() -> None:
    """Verifies multi-version key rotation where encrypt uses active version, and decrypt reads version byte."""
    # Service configured with version 1 as active
    service_v1: CryptoService = CryptoService(keys={1: TEST_KEY_V1}, active_version=1, blind_index_key=TEST_BLIND_KEY)
    token_v1: str = service_v1.encrypt("+1111111111", aad=b"aad")

    # New service deployed with version 2 as active, but retaining version 1 for reads
    service_v2: CryptoService = CryptoService(
        keys={1: TEST_KEY_V1, 2: TEST_KEY_V2},
        active_version=2,
        blind_index_key=TEST_BLIND_KEY,
    )
    token_v2: str = service_v2.encrypt("+2222222222", aad=b"aad")

    # Service v2 can decrypt legacy version 1 ciphertext
    assert service_v2.decrypt(token_v1, aad=b"aad") == "+1111111111"

    # Service v2 can decrypt version 2 ciphertext
    assert service_v2.decrypt(token_v2, aad=b"aad") == "+2222222222"

    # Verify first byte corresponds to active version
    raw_v1: bytes = base64.urlsafe_b64decode(token_v1 + "==")
    raw_v2: bytes = base64.urlsafe_b64decode(token_v2 + "==")
    assert raw_v1[0] == 1
    assert raw_v2[0] == 2


def test_blind_index_determinism_and_normalization() -> None:
    """Verifies HMAC-SHA256 blind indexing is deterministic, 64 hex chars, and normalizes phone input."""
    crypto: CryptoService = CryptoService(keys={1: TEST_KEY_V1}, active_version=1, blind_index_key=TEST_BLIND_KEY)

    idx1: str = crypto.blind_index("+14155552671")
    idx2: str = crypto.blind_index("+1 (415) 555-2671")  # Format variations normalize to same E.164
    idx3: str = crypto.blind_index("+1 415.555.2671")

    assert len(idx1) == 64  # SHA-256 hex digest length
    assert idx1 == idx2
    assert idx1 == idx3

    # Different number produces different hash
    idx_different: str = crypto.blind_index("+14155559999")
    assert idx1 != idx_different


def test_phone_masking() -> None:
    """Ensures phone numbers are masked properly to protect PII."""
    assert CryptoService.mask_phone("+14155552671") == "+1******2671"
    assert CryptoService.mask_phone("+447911123456") == "+4*******3456"
    assert CryptoService.mask_phone("123") == "***"
