# Senior Backend & System Architecture Patterns

Comprehensive technical guidelines for developing and reviewing components within this repository.

---

## 1. Clean Layered Architecture

```
┌────────────────────────────────────────────────────────┐
│                   HTTP Transport Layer                 │
│               app/api/v1/endpoints/*.py                │
│    (FastAPI routers, dependency injection, Pydantic)   │
└───────────────────────────┬────────────────────────────┘
                            │ Calls Service methods
                            ▼
┌────────────────────────────────────────────────────────┐
│                   Domain Service Layer                 │
│                   app/services/*.py                    │
│   (Business logic, lifecycle hooks, transactions,      │
│     cache invalidation, PII cryptography)              │
└───────────────────────────┬────────────────────────────┘
                            │ Calls Repository methods
                            ▼
┌────────────────────────────────────────────────────────┐
│                  Repository Persistence Layer          │
│                 app/repositories/*.py                  │
│    (SQLAlchemy 2.0 Async, dynamic filtering,           │
│     keyset cursor pagination, flushes)                 │
└───────────────────────────┬────────────────────────────┘
                            │ Interacts with DB
                            ▼
┌────────────────────────────────────────────────────────┐
│                    PostgreSQL / MySQL                  │
└────────────────────────────────────────────────────────┘
```

### Layer Dependency Rules:
1. **Controllers (API endpoints)** only depend on **Services** via `Annotated[Service, Depends(get_service)]`.
2. **Services** depend on **Repositories**, **Cache**, and **CryptoService**.
3. **Repositories** depend on **AsyncSession** and **SQLAlchemy Models**.
4. **Never invert dependencies**: Controllers must never query repositories directly. Repositories must never access HTTP requests.

---

## 2. Template Method Pattern in Services

Instead of rewriting `create`, `get`, `update`, and `delete` in every domain service, all generic mechanics are encapsulated in `BaseService`. Domain services only override **lifecycle hooks**:

```python
class BaseService:
    async def create(self, payload):
        entity = await self.before_create(payload)      # [HOOK] Validation, UUID, Encryption
        await self.repo.create(entity)                  # Persistence
        await self.cache.invalidate_lists()             # Cache Eviction
        await self.db.commit()                          # Transaction Commit Boundary
        return await self.to_read_schema(entity)        # [HOOK] Decryption, Masking
```

### Pluggable Lifecycle Hooks:
- **`before_create(payload) -> Model`**: Uniqueness checks (email, phone blind index), client UUID generation, PII encryption.
- **`before_update(entity, payload) -> None`**: Partial update application, selective field re-encryption, conflict checks.
- **`to_read_schema(entity) -> ReadSchema`**: Plaintext decryption using AAD, phone masking (`+1******2671`).
- **`to_cache_dict(entity) -> Dict`**: Masked representation serialized for Redis storage.

---

## 3. Keyset (Cursor) Pagination vs Offset Degradation

Deep offset pagination (`OFFSET 100000 LIMIT 20`) forces the database engine to scan 100,020 rows in $O(N)$ time, discarding the first 100,000.

Our architecture enforces **Keyset (cursor) B-Tree seeks**:
- Composite cursor: `(created_at, id)` encoded as URL-safe Base64.
- SQL seek clause:
  ```sql
  WHERE (created_at > :cursor_dt)
     OR (created_at = :cursor_dt AND id > :cursor_id)
  ORDER BY created_at ASC, id ASC
  LIMIT :safe_limit + 1
  ```
- Result: **Constant $O(1)$ lookup time** regardless of whether page is 1 or 1,000,000.

---

## 4. Distributed Redis Caching Architecture

1. **Single Item Caching**:
   - Key: `cache:<prefix>:item:<id>` (NO hash tag).
   - Distributed evenly across all master nodes via MurmurHash3.
2. **List Invalidation via Namespace Versioning**:
   - Key: `cache:<prefix>:list:v{version}:{hash}`.
   - Invalidation: `await redis.incr(f"cache:{prefix}:list_ver")` in **$O(1)$ time**.
   - Eliminates `CROSSSLOT` multi-key DEL failures and unbounded Set tracking.
3. **Cache Stampede Mutex (Dogpiling)**:
   - On a cache miss for hot keys, a single worker acquires a `SET NX PX 5000` distributed lock.
   - Waiting workers sleep 50ms and re-fetch from cache, protecting the primary database.
4. **Negative Caching**:
   - Missing IDs are cached for 30s with `NEGATIVE_CACHE_SENTINEL = {"__not_found__": True}` to thwart penetration DDoS attacks.

---

## 5. Zero-Trust PII Cryptography

1. **AES-256-GCM Authenticated Encryption**:
   - Random 12-byte nonce generated per encryption (never reused).
   - 16-byte authentication tag verifies integrity and authenticity.
2. **AAD Context Binding**:
   - Always bind ciphertext to entity identity: `aad = f"users.phone:{user_id}".encode("utf-8")`.
   - Prevents ciphertext transplantation attacks (stealing encrypted phone from user A and pasting into user B).
3. **HMAC-SHA256 Blind Indexing**:
   - Separate secret key computes deterministic HMAC of normalized plaintext.
   - Allows exact match queries (`SELECT * FROM users WHERE phone_blind_index = :hash`) without decrypting database records.
