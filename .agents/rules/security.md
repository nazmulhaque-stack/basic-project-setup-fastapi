# Security & Cryptography Rules

1. **PII Cryptography (AES-256-GCM AEAD)**:
   - Encrypt sensitive PII with AES-256-GCM using `CryptoService.encrypt(plain, aad=aad)`.
   - Never omit Additional Authenticated Data (`aad`). Always bind to entity identity: `f"{table}.{field}:{entity_id}"`.
   - Always generate a fresh 96-bit (12-byte) random nonce per encryption via `os.urandom(12)`. Nonces must never be reused.
   - For database lookups on encrypted columns, always query via `phone_blind_index` computed by `CryptoService.blind_index(plain)`.

2. **Zero-Downtime Key Rotation**:
   - Pack wire format tokens with version prefix: `[version(1B) || nonce(12B) || ciphertext+tag(N+16B)]`.
   - Support multiple active decryption keys via `ENCRYPTION_KEYS` JSON mapping.
   - Decryption inspects Byte 0 to resolve the historical key; all new encryptions write using `ACTIVE_KEY_VERSION`.

3. **Secret & Key Protection**:
   - Never hardcode secrets, private keys, or API tokens.
   - Load secrets exclusively via environment variables (`Settings` from `app.core.config`).
   - Block `.env`, `*.key`, `*.pem`, and `*.cert` from Git commits.

4. **Rate Limiting & Proxy Trust**:
   - Never trust raw `X-Forwarded-For` headers without verifying that the direct peer IP is within `TRUSTED_PROXIES`.
   - Protect rate limiting keys with hash tags: `rl:{<scope>:<identity>}:<window>` to ensure single slot pinning in Redis Cluster.

5. **PII Masking at Serialization Boundary**:
   - Never return plaintext PII in public read schemas or Redis cached dictionaries.
   - Apply deterministic masking before serializing (`+1******2671`).
