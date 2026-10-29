# Security & Cryptography Rules

1. **PII Cryptography**:
   - Encrypt sensitive PII with AES-256-GCM using `CryptoService.encrypt(plain, aad=aad)`.
   - Never omit Additional Authenticated Data (`aad`). Always bind to `f"{table}.{field}:{entity_id}"`.
   - For database lookups on encrypted columns, always query via `phone_blind_index` computed by `CryptoService.blind_index(plain)`.

2. **Secret & Key Protection**:
   - Never hardcode secrets, private keys, or API tokens.
   - Load secrets exclusively via environment variables (`Settings` from `app.core.config`).
   - Block `.env`, `*.key`, `*.pem`, and `*.cert` from Git commits.

3. **Rate Limiting & Proxy Trust**:
   - Never trust raw `X-Forwarded-For` headers without verifying that the direct peer IP is within `TRUSTED_PROXIES`.
   - Protect rate limiting keys with hash tags: `rl:{<scope>:<identity>}:<window>` to ensure single slot pinning in Redis Cluster.
