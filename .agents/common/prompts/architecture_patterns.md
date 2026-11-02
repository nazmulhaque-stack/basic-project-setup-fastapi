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
│               PgBouncer -> PostgreSQL 16               │
└────────────────────────────────────────────────────────┘
```

### Layer Dependency Rules:
1. **Controllers (API endpoints)** only depend on **Services** via `Annotated[Service, Depends(get_service)]`.
2. **Services** depend on **Repositories**, **Cache**, **CryptoService**, and **ElasticsearchService**.
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
        await self.after_create(entity)                 # [HOOK] Post-commit side effects
        return await self.to_read_schema(entity)        # [HOOK] Decryption, Masking
```

### Pluggable Lifecycle Hooks:
- **`before_create(payload) -> Model`**: Uniqueness checks (email, phone blind index), client UUID generation, PII encryption.
- **`before_update(entity, payload) -> None`**: Partial update application, selective field re-encryption, conflict checks.
- **`before_delete(entity) -> None`**: Custom cascading rules prior to deletion. No-op by default.
- **`after_create(entity) -> None`** / **`after_update(entity, payload) -> None`** / **`after_delete(entity) -> None`**: Fire only after the transaction has committed — the entity is durably persisted. Use for side effects that must not run if the mutation rolls back (notifications, audit logs, follow-up jobs). No-op by default.
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
- Result: **Constant $O(\log N)$ lookup time** regardless of whether page is 1 or 1,000,000.

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
5. **Dynamic TTL Jitter**:
   - ±10% randomized TTL jitter prevents synchronized cache stampedes.

---

## 5. Zero-Trust PII Cryptography

1. **AES-256-GCM Authenticated Encryption**:
   - Random 12-byte nonce generated per encryption (never reused).
   - 16-byte authentication tag verifies integrity and authenticity.
2. **AAD Context Binding**:
   - Always bind ciphertext to entity identity: `aad = f"users.phone:{user_id}".encode("utf-8")`.
   - Prevents ciphertext transplantation attacks (stealing encrypted phone from user A and pasting into user B).
3. **HMAC-SHA256 Blind Indexing**:
   - Dedicated 32-byte secret key computes deterministic HMAC-SHA256 of normalized plaintext.
   - Allows exact match queries (`SELECT * FROM users WHERE phone_blind_index = :hash`) and unique indexes without decrypting database records.
4. **Zero-Downtime Key Rotation**:
   - Wire format stores key version in Byte 0: `[version(1B) || nonce(12B) || ciphertext+tag(N+16B)]`.
   - System loads all valid keys from `ENCRYPTION_KEYS` JSON, writes new records with `ACTIVE_KEY_VERSION`.

---

## 6. Database Connection Multiplexing (PgBouncer)

1. **Transaction Pooling Mode**:
   - PgBouncer holds idle client connections, binding a PostgreSQL server socket ONLY for the duration of a transaction (`BEGIN ... COMMIT`).
2. **Mandatory Statement Cache Sizing**:
   - `statement_cache_size = 0` and `prepared_statement_cache_size = 0` in asyncpg `connect_args`.
   - Prepared statements in asyncpg are per-connection and fail when PgBouncer switches physical connections between transactions.
3. **Connection Budget Math for 100k Concurrency**:
   - 16 Hosts × 8 Workers = 128 ASGI Worker Processes.
   - Each process holds `pool_size(5) + max_overflow(5) = 10` sockets $\rightarrow$ 1,280 client connections to PgBouncer.
   - PgBouncer multiplexes these down to `default_pool_size = 80` backend sockets to PostgreSQL 16.

---

## 7. CQRS Full-Text Search Engine (Elasticsearch 8)

1. **CQRS Architecture**:
   - PostgreSQL is the authoritative system of record for all writes.
   - Elasticsearch asynchronously indexes documents for full-text search, fuzzy matching, and autocomplete.
2. **Graceful Fallback**:
   - Implemented via `AsyncElasticsearchService` with fallback to `NullElasticsearch` if the cluster is unreachable or disabled.

---

## 8. 100,000 (1 Lakh) Concurrent Requests System Architecture

1. **Network Ingress & Socket Tuning**:
   - Layer 4 (Anycast/ECMP) balances TCP streams across 13–20 Nginx reverse proxy nodes.
   - Linux kernel tuning (`deploy/sysctl.conf`): `somaxconn = 65535`, `tcp_max_syn_backlog = 65535`, `tcp_tw_reuse = 1`, `nofile = 1048576`.
2. **Reverse Proxy (Nginx)**:
   - `epoll` event loop with `worker_connections 65535`.
   - `limit_conn` (100/IP) & `limit_req` (100r/s burst=50 nodelay).
   - Upstream keepalive pools (128 sockets) to eliminate per-request TCP handshakes.
3. **ASGI Application Pool**:
   - Gunicorn master supervisor with 1 worker per physical core.
   - Uvicorn with C-based `uvloop` (epoll) and `httptools`.
   - Binary serialization via `ORJSONResponse`.
   - Distributed Lua rate limiting via `EVALSHA` on Redis Cluster.
