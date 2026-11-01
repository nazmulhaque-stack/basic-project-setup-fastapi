# 📖 End-to-End Execution Flow & Developer Implementation Guide

Comprehensive architectural execution trace and complete developer implementation guide for the **FastAPI High-Performance 100k Concurrency Backend Architecture**.

> 🏛️ **System Architecture & Infrastructure Specifications**: For hardware sizing math, 100,000 connection budget proofs, Docker/Kubernetes manifests, and benchmark commands, see [README.md](file:///home/nazmul/Downloads/fastapi-repo-pattern/README.md).

---

## 📑 Table of Contents

1. **[Part 1: User Request Execution Lifecycle](#part-1-user-request-execution-lifecycle)** — The complete, step-by-step path of incoming HTTP requests across all primary operational modes:
   - [1.1 Complete Write Path: `POST /api/v1/users`](#11-complete-write-path-post-apiv1users) (AES-256-GCM AEAD, blind index, cache invalidation)
   - [1.2 Complete Read Path: `GET /api/v1/users/{id}`](#12-complete-read-path-get-apiv1usersid) (Distributed mutex locking, negative caching)
   - [1.3 Keyset Cursor Pagination Path: `GET /api/v1/users`](#13-keyset-cursor-pagination-path-get-apiv1users) ($O(\log N)$ composite seek)
   - [1.4 Elasticsearch Search & Async Logging Lifecycle](#14-elasticsearch-search--async-logging-lifecycle) (CQRS full-text search and non-blocking log buffer)
   - [1.5 Relational & Nested Query Execution Lifecycle](#15-relational--nested-query-execution-lifecycle) (`selectinload` eager loading vs $N+1$ query prevention)
2. **[Part 2: Developer Implementation Workflow](#part-2-developer-implementation-workflow)** — A practical, deep guide for developers on building features:
   - [2.1 File Modification Checklist](#21-file-modification-checklist)
   - [2.2 In-Depth Step-by-Step Code Examples](#22-in-depth-step-by-step-code-examples)
   - [2.3 Unit of Work & Multi-Repository Transactions](#23-unit-of-work--multi-repository-transactions)
   - [2.4 Writing Automated Tests](#24-writing-automated-tests-teststest_orderspy)
   - [2.5 Nested Tables & Entity Relationships Guide (Category in Product)](#25-nested-tables--entity-relationships-guide-category-in-product)
     - [2.5.1 The `lazy="raise"` Rule in Async SQLAlchemy](#251-the-lazyraise-rule-in-async-sqlalchemy)
     - [2.5.2 Step 1: Model Definition with Foreign Keys & Relationships](#252-step-1-model-definition-with-foreign-keys--relationships)
     - [2.5.3 Step 2: Nested Pydantic Schemas (Embedded DTOs & Validation)](#253-step-2-nested-pydantic-schemas-embedded-dtos--validation)
     - [2.5.4 Step 3: Repository Eager Loading (`selectinload` vs `joinedload`)](#254-step-3-repository-eager-loading-selectinload-vs-joinedload)
     - [2.5.5 Step 4: Service Layer Validation & Cross-Entity Cache Invalidation](#255-step-4-service-layer-validation--cross-entity-cache-invalidation)
     - [2.5.6 Step 5: Dynamic CRUDRouter & Custom Nested Endpoints](#256-step-5-dynamic-crudrouter--custom-nested-endpoints)
     - [2.5.7 Step 6: Database Migrations with Alembic](#257-step-6-database-migrations-with-alembic)
     - [2.5.8 Step 7: Testing Relationships & Preventing N+1 Regressions](#258-step-7-testing-relationships--preventing-n1-regressions)
3. **[Part 3: High-Performance Infrastructure & State Engines (Redis, Elasticsearch & Apache Kafka)](#part-3-high-performance-infrastructure--state-engines-redis-elasticsearch--apache-kafka)** — Deep mechanics of the caching, search, and streaming pipelines:
   - [3.1 Redis 7 Cluster Architecture & Workflows](#31-redis-7-cluster-architecture--workflows)
     - [3.1.1 Distributed Mutex Locking & Stampede Protection](#311-distributed-mutex-locking--stampede-protection)
     - [3.1.2 Sub-Millisecond Point Lookups & Hot Caching](#312-sub-millisecond-point-lookups--hot-caching)
     - [3.1.3 Dynamic Namespace Versioning ($O(1)$ Invalidation)](#313-dynamic-namespace-versioning-o1-invalidation)
     - [3.1.4 Request Idempotency Engine](#314-request-idempotency-engine)
     - [3.1.5 Verification Challenges & One-Time Tokens](#315-verification-challenges--one-time-tokens)
     - [3.1.6 Sliding-Window Rate Limiting & Atomic Quotas](#316-sliding-window-rate-limiting--atomic-quotas)
     - [3.1.7 Negative Caching Sentinel](#317-negative-caching-sentinel)
     - [3.1.8 Cluster High-Availability & Standalone Fallback](#318-cluster-high-availability--standalone-fallback)
   - [3.2 Elasticsearch 8 Search & Analytics Architecture & Workflows](#32-elasticsearch-8-search--analytics-architecture--workflows)
     - [3.2.1 CQRS Read-Model Inverted Indexing](#321-cqrs-read-model-inverted-indexing)
     - [3.2.2 BM25 Full-Text Relevance Scoring & Fuzzy Matching](#322-bm25-full-text-relevance-scoring--fuzzy-matching)
     - [3.2.3 Columnar Doc Values & Real-Time Faceted Aggregations](#323-columnar-doc-values--real-time-faceted-aggregations)
     - [3.2.4 Deep Pagination via Keyset & `search_after`](#324-deep-pagination-via-keyset--search_after)
     - [3.2.5 High-Throughput Non-Blocking Structured Logging](#325-high-throughput-non-blocking-structured-logging)
     - [3.2.6 Log Search API & Histogram Aggregations](#326-log-search-api--histogram-aggregations)
   - [3.3 Apache Kafka Microservices Event-Driven Streaming](#33-apache-kafka-microservices-event-driven-streaming)
     - [3.3.1 The Dual-Write Problem & Transactional Outbox Pattern](#331-the-dual-write-problem--transactional-outbox-pattern)
     - [3.3.2 Asynchronous Outbox Dispatcher Daemon](#332-asynchronous-outbox-dispatcher-daemon)
     - [3.3.3 CloudEvents Schema Standard](#333-cloudevents-schema-standard)
     - [3.3.4 Producer Architecture](#334-producer-architecture)
     - [3.3.5 Consumer Architecture & Strict Manual Offset Commit](#335-consumer-architecture--strict-manual-offset-commit)
     - [3.3.6 Consumer Retry Policy & Dead Letter Queue](#336-consumer-retry-policy--dead-letter-queue)
     - [3.3.7 Inventory Reservation Saga & TTL Sweeper](#337-inventory-reservation-saga--ttl-sweeper)
     - [3.3.8 Standalone Background Worker Daemon](#338-standalone-background-worker-daemon)
4. **[Part 4: Database Auto-Initialization, Safe Migrations & Idempotent Seeders](#part-4-database-auto-initialization-safe-migrations--idempotent-seeders)** — Production-grade schema management:
   - [4.1 Safe Table Inspection & Auto-Creation Mechanics](#41-safe-table-inspection--auto-creation-mechanics)
   - [4.2 Alembic Migration Execution & Advisory Locks](#42-alembic-migration-execution--advisory-locks)
   - [4.3 Idempotent Seeder Engine & Topological Dependency Resolution](#43-idempotent-seeder-engine--topological-dependency-resolution)
   - [4.4 Unified Database CLI Reference](#44-unified-database-cli-reference)
5. **[Part 5: Container-Wise & Environment Configuration Guide](#part-5-container-wise--environment-configuration-guide)** — Operational deployment guide:
   - [5.1 Host vs. Docker Networking & Port Matrix](#51-host-vs-docker-networking--port-matrix)
   - [5.2 Environment Variable Dictionary (`.env`)](#52-environment-variable-dictionary-env)
   - [5.3 Dynamic Service URL Assembly in Pydantic Settings](#53-dynamic-service-url-assembly-in-pydantic-settings)

---

# Part 1: User Request Execution Lifecycle

This section tracks the execution flow across five primary operations:
- **Write Path**: `POST /api/v1/users` (Registration with AES-256-GCM encryption, blind indexing, and cache invalidation)
- **Read Path**: `GET /api/v1/users/{id}` (Cache-aside with stampede mutex locks and negative caching)
- **Keyset Pagination Path**: `GET /api/v1/users?limit=50&cursor=...` ($O(\log N)$ B-Tree cursor seek)
- **Elasticsearch Search & Ingestion Path**: `GET /api/v1/products/search?q=...` & Background Ingestion Buffer
- **Nested Relational Path**: `GET /api/v1/products/{id}/detail` (Eager loading `category_rel` via `selectinload`)

<p align="center">
  <img src="assets/architecture.png" alt="FastAPI High-Performance 100k Concurrency Architecture" width="100%" />
</p>

---

## 1.1 Complete Write Path: `POST /api/v1/users`

<p align="center">
  <img src="assets/sequence_flow.png" alt="Complete Write Path Execution Lifecycle" width="100%" />
</p>

```
[Client] 
   │ HTTP POST /api/v1/users
   ▼
[Step 1: Network Ingress & Nginx] ──► deploy/nginx.conf
   │ Upstream keepalive TCP socket
   ▼
[Step 2: Process & ASGI Parsing] ───► deploy/gunicorn.conf.py & Uvicorn (uvloop)
   │ ASGI Scope Dictionary
   ▼
[Step 3: Tracing Middleware] ───────► app/main.py: request_id_and_tracing_middleware()
   │ Injects request.state.request_id
   ▼
[Step 4: Rate Limiting & Proxy IP] ─► app/services/redis_rate_limit.py: RateLimiter.__call__()
   │ Redis Lua EVALSHA verification
   ▼
[Step 5: Database Unit of Work] ────► app/core/database.py: get_db()
   │ AsyncSession instantiated (autoflush=False)
   ▼
[Step 6: Pydantic Validation] ──────► app/schemas/user.py: UserCreate.validate_e164_phone()
   │ Validated payload
   ▼
[Step 7: Endpoint Controller] ──────► app/api/v1/users.py: Dynamic CRUDRouter
   │ Delegates to service
   ▼
[Step 8: Domain Business Service] ──► app/services/user.py: UserService.create()
   │ ├─ 8a. Conflict checks: await repo.get_by(email=...)
   │ ├─ 8b. AES-256-GCM AAD encryption: app/core/security.py: CryptoService.encrypt()
   │ ├─ 8c. Blind index generation: app/core/security.py: CryptoService.blind_index()
   │ └─ 8d. Entity instantiation: User(id=user_id, ...)
   ▼
[Step 9: Generic Repository] ───────► app/repositories/base.py: BaseRepository.create()
   │ session.add(user) & await session.flush()
   ▼
[Step 10: Connection Multiplexer] ──► PgBouncer (Transaction Mode) ──► PostgreSQL 16
   │ SQL executed, UNIQUE constraints enforced; transaction remains open
   ▼
[Step 11: Cache Invalidation] ──────► app/services/base.py: DynamicRedisCache.invalidate_lists()
   │ Redis Cluster: INCR cache:users:list_ver
   ▼
[Step 12: Output Schema & Masking] ─► app/services/user.py: to_read_schema()
   │ Decrypts phone, applies mask (+1*******2671), returns UserRead
   ▼
[Step 13: Transaction Commit] ──────► app/core/database.py: get_db() exit
   │ await session.commit() ──► PgBouncer releases PostgreSQL backend connection
   ▼
[Step 14: Fast Serialization] ─────► app/main.py (ORJSONResponse)
   │ Binary JSON + X-Request-ID + X-RateLimit-* headers
   ▼
[Client receives HTTP 201 Created]
```

---

### Step-by-Step Function Execution Details (Write Path)

#### Step 1: Ingress & Reverse Proxy
- **File**: [`deploy/nginx.conf`](file:///home/nazmul/Downloads/fastapi-repo-pattern/deploy/nginx.conf)
- **Directive / Function**:
  - `limit_conn conn_limit_per_ip 100`: Rejects excess open TCP connections from the client IP.
  - `limit_req zone=req_limit_per_ip burst=50 nodelay`: Checks the leaky-bucket rate limiter.
  - `proxy_pass http://app_backend`: Selects an idle persistent keepalive socket from `upstream app_backend` (pool of 128 sockets).
  - Injects headers: `X-Real-IP`, `X-Forwarded-For`, `X-Request-ID`.

#### Step 2: ASGI Event Loop & Worker Dispatch
- **File**: [`deploy/gunicorn.conf.py`](file:///home/nazmul/Downloads/fastapi-repo-pattern/deploy/gunicorn.conf.py)
- **Component**: Gunicorn Supervisor $\rightarrow$ Uvicorn Worker (`uvloop` + `httptools`)
- **Action**:
  - The OS network socket event triggers an `epoll` notification in `uvloop`.
  - `httptools` parses the HTTP byte stream into the ASGI scope:
    ```python
    scope = {
        "type": "http",
        "method": "POST",
        "path": "/api/v1/users",
        "headers": [(b"host", b"..."), (b"x-forwarded-for", b"203.0.113.1"), ...],
    }
    ```
  - Dispatches to `FastAPI.__call__(scope, receive, send)` asynchronously without thread blocking.

#### Step 3: Tracing & Request ID Middleware
- **File**: [`app/main.py`](file:///home/nazmul/Downloads/fastapi-repo-pattern/app/main.py#L48)
- **Function**: `request_id_and_tracing_middleware(request: Request, call_next)`
- **Action**:
  - Checks `request.headers.get("X-Request-ID")`. If absent, generates `str(uuid.uuid4())`.
  - Attaches to request state: `request.state.request_id = req_id`.
  - Captures start time: `start_time = time.perf_counter()`.
  - Passes execution to the inner route handler: `response = await call_next(request)`.

#### Step 4: Rate Limiting & Proxy IP Resolution
- **File**: [`app/services/redis_rate_limit.py`](file:///home/nazmul/Downloads/fastapi-repo-pattern/app/services/redis_rate_limit.py#L82)
- **Class / Method**: `RateLimiter.__call__(self, request: Request, response: Response)`
- **Sub-functions**:
  1. `resolve_client_ip(request, settings.TRUSTED_PROXIES)`:
     - Inspects `request.client.host` (the direct TCP socket peer).
     - Validates peer against configured CIDRs (e.g. `127.0.0.1`, `10.0.0.0/8`).
     - Extracts the genuine client IP from `X-Forwarded-For`. Untrusted peers cannot spoof IP headers.
  2. Constructs Redis cluster slot-pinned key:
     `rl:{users:ip:203.0.113.1}:1698403200`
  3. Executes atomic Lua script in Redis via `EVALSHA`:
     ```lua
     local current = redis.call('INCR', KEYS[1])
     if current == 1 then
         redis.call('EXPIRE', KEYS[1], ARGV[1])
     end
     return {current, redis.call('TTL', KEYS[1])}
     ```
  4. Injects HTTP headers:
     - `X-RateLimit-Limit: 100`
     - `X-RateLimit-Remaining: 99`
     - `X-RateLimit-Reset: 1698403260`
  5. If `current > limit`, raises `HTTPException` with status 429 and `Retry-After` header.

#### Step 5: Database Unit of Work Initialization
- **File**: [`app/core/database.py`](file:///home/nazmul/Downloads/fastapi-repo-pattern/app/core/database.py#L77)
- **Function**: `get_db(request: Request) -> AsyncGenerator[AsyncSession, None]`
- **Action**:
  - Fetches the engine session factory: `session_factory = request.app.state.session_factory`.
  - Opens context manager: `async with session_factory() as session:`.
  - Produces an `AsyncSession` with:
    - `expire_on_commit = False`: Prevents lazy-load errors post-commit.
    - `autoflush = False`: Prevents unexpected intermediate SQL flushes.
  - `yield session`: Suspends here while the endpoint and service execute.

#### Step 6: Request Payload Validation & Normalization
- **File**: [`app/schemas/user.py`](file:///home/nazmul/Downloads/fastapi-repo-pattern/app/schemas/user.py#L22)
- **Class**: `UserCreate(BaseModel)`
- **Validators**:
  - `email: EmailStr`: Validates RFC 5322 email formatting.
  - `@field_validator("phone")`: Invokes `validate_e164_phone(v)`.
    - Strips whitespace, hyphens, parentheses.
    - Matches against regex: `^\+[1-9]\d{1,14}$`.
    - Normalizes to canonical format: `+14155552671`.

#### Step 7: HTTP Controller Dispatch (CRUDRouter)
- **File**: [`app/api/v1/users.py`](file:///home/nazmul/Downloads/fastapi-repo-pattern/app/api/v1/users.py#L8)
- **Component**: `CRUDRouter(model=User, service=get_user_service, ...)`
- **Action**:
  - Dynamically routes `POST /api/v1/users` without boilerplate explicit handler functions.
  - Receives the validated Pydantic model (`UserCreate`).
  - Resolves `UserService` via `app/api/deps.py`:
    ```python
    UserService(
        repo=UserRepository(session),
        cache=DynamicRedisCache("users", redis),
        crypto=request.app.state.crypto
    )
    ```
  - Calls: `await service.create_user(payload)`.

#### Step 8: Domain Business Service & Cryptographic Pipeline
- **File**: [`app/services/user.py`](file:///home/nazmul/Downloads/fastapi-repo-pattern/app/services/user.py#L42)
- **Method**: `UserService.create_user(payload: UserCreate)`
- **Execution Flow**:
  1. **Lifecycle Hook**: `await self.before_create(payload)`:
     - Pre-generates UUID: `user_id = uuid.uuid4()`.
     - Validates email uniqueness:
       `existing = await self.repo.get_by(email=payload.email)`
       If exists, raises `ConflictError("User with this email already exists")`.
     - Computes HMAC-SHA256 blind index:
       `phone_bi = self.crypto.blind_index(payload.phone)`
     - Validates phone uniqueness without decrypting database records:
       `existing_phone = await self.repo.get_by_blind_index(phone_bi)`
       If exists, raises `ConflictError("User with this phone number already exists")`.
  2. **AES-256-GCM AEAD Encryption** ([`app/core/security.py`](file:///home/nazmul/Downloads/fastapi-repo-pattern/app/core/security.py#L85)):
     - Method: `CryptoService.encrypt(plaintext=payload.phone, aad=f"users.phone:{user_id}".encode("utf-8"))`
     - Generates 12 random bytes via `os.urandom(12)`.
     - Retrieves cipher for active key version: `self._ciphers[self._active_version]`.
     - Computes AES-256-GCM ciphertext + 16-byte Poly1305/GCM authentication tag bound to AAD.
     - Binary packing: `[active_version(1B) || nonce(12B) || ciphertext+tag(N+16B)]`.
     - Encodes to URL-safe Base64 string without padding.
  3. **Entity Construction**:
     ```python
     user = User(
         id=user_id,
         name=payload.name,
         email=payload.email,
         phone_encrypted=enc_phone,
         phone_blind_index=phone_bi,
         role=payload.role,
     )
     ```
  4. Invokes repository: `await self.repo.create(user)`.

#### Step 9: Generic Repository & SQL Flush
- **File**: [`app/repositories/base.py`](file:///home/nazmul/Downloads/fastapi-repo-pattern/app/repositories/base.py#L95)
- **Class / Method**: `BaseRepository.create(entity: ModelType) -> ModelType`
- **Action**:
  - `self.session.add(entity)`: Adds entity to the SQLAlchemy unit of work identity map.
  - `await self.session.flush()`:
    - Generates SQL: `INSERT INTO users (id, name, email, phone_encrypted, phone_blind_index, role, created_at, updated_at) VALUES (...)`.
    - Dispatches SQL query over database socket to PgBouncer.
    - **Crucial**: Does NOT call `session.commit()`. The transaction remains open in PostgreSQL.

#### Step 10: PgBouncer Multiplexing & PostgreSQL Execution
- **Components**: PgBouncer 1.21+ (Transaction Mode) $\rightarrow$ PostgreSQL 16
- **Action**:
  - PgBouncer assigns an available pre-warmed backend connection from its 80-connection pool.
  - PostgreSQL executes `INSERT`, checking unique B-Tree indexes:
    - `users_email_key`
    - `ix_users_phone_blind_index`
  - PostgreSQL returns the inserted row back to the `asyncpg` driver.

#### Step 11: Distributed Cache Invalidation
- **File**: [`app/services/base.py`](file:///home/nazmul/Downloads/fastapi-repo-pattern/app/services/base.py)
- **Method**: `DynamicRedisCache.invalidate_lists()`
- **Action**:
  - Executes: `await self.redis.incr(f"cache:{self.namespace}:list_ver")`.
  - Redis Cluster atomically increments the list version counter in $O(1)$ time.
  - All existing cached list pages across the cluster are invalidated instantly without scanning or deleting keys.

#### Step 12: Output Serialization & PII Masking
- **File**: [`app/services/user.py`](file:///home/nazmul/Downloads/fastapi-repo-pattern/app/services/user.py#L94)
- **Method**: `UserService.to_read_schema(entity: User) -> UserRead`
- **Action**:
  - Decrypts phone: `self.crypto.decrypt(entity.phone_encrypted, aad=f"users.phone:{entity.id}".encode("utf-8"))`.
  - Masks phone via `mask_phone(plain_phone)`: `+14155552671` $\rightarrow$ `+1*******2671`.
  - Returns validated Pydantic model: `UserRead`.

#### Step 13: Transaction Commit (Unit of Work Exit)
- **File**: [`app/core/database.py`](file:///home/nazmul/Downloads/fastapi-repo-pattern/app/core/database.py#L83)
- **Context Manager**: Resumes execution inside `get_db()`:
  ```python
  try:
      yield session
      await session.commit()  # <-- Commits PostgreSQL transaction
  except Exception:
      await session.rollback()
      raise
  finally:
      await session.close()
  ```
- PgBouncer receives `COMMIT`, completes the PostgreSQL transaction, and returns the backend physical connection to the pool for the next request.

#### Step 14: Response Serialization & Network Transmission
- **File**: [`app/main.py`](file:///home/nazmul/Downloads/fastapi-repo-pattern/app/main.py)
- **Action**:
  - `ORJSONResponse` serializes `UserRead` into binary JSON.
  - Tracing middleware attaches `X-Request-ID` and logs the execution duration.
  - Uvicorn transmits bytes over keepalive socket $\rightarrow$ Nginx $\rightarrow$ Client receives `HTTP 201 Created`.

---

## 1.2 Complete Read Path: `GET /api/v1/users/{id}`

<p align="center">
  <img src="assets/read_cache_flow.png" alt="Cache-Aside Read Path & Stampede Mutex Protection" width="100%" />
</p>

When a client queries a user by ID, the execution follows this decision tree:

```
[Client] ──► GET /api/v1/users/{id}
   │
   ▼
[app/api/v1/users.py: CRUDRouter GET /{id}]
   │
   ▼
[app/services/user.py: UserService.get_by_id()]
   │
   ▼
[app/services/base.py: get_by_id()]
   │
   ├─► 1. Query Redis: await cache.get_item(id)
   │      │
   │      ├─► [HIT] Item found in Redis
   │      │   └─► Deserialize JSON ──► Return UserRead (0 DB queries, < 1ms)
   │      │
   │      ├─► [NEGATIVE HIT] Found {"__not_found__": true}
   │      │   └─► Raise NotFoundError (HTTP 404, 0 DB queries, < 1ms)
   │      │
   │      └─► [MISS] Key not in Redis
   │          │
   │          ▼
   │      [Cache Stampede Distributed Mutex Lock]
   │      SET cache:users:lock:{id} "1" NX PX 5000
   │          │
   │          ├─► [LOCK ACQUIRED - Worker A]
   │          │   ├─ Query DB: await repo.get(id)
   │          │   ├─ If Not Found in DB:
   │          │   │  └─ Write {"__not_found__": true} (TTL 30s) ──► Raise 404
   │          │   ├─ If Found in DB:
   │          │   │  ├─ Serialize with masked phone: to_cache_dict()
   │          │   │  └─ Write to Redis with ±10% jitter (TTL 270-330s)
   │          │   └─ Release lock: DEL cache:users:lock:{id}
   │          │
   │          └─► [LOCK BUSY - Workers B, C... N]
   │              ├─ Sleep 50ms
   │              └─ Re-read Redis cache (now populated by Worker A!)
   ▼
[Return UserRead (HTTP 200 OK)]
```

---

## 1.3 Keyset Cursor Pagination Path: `GET /api/v1/users`

```sql
-- Executed by app/repositories/base.py: list_keyset()
SELECT id, name, email, phone_encrypted, phone_blind_index, role, created_at, updated_at
FROM users
WHERE (created_at, id) < ('2026-10-28 09:00:00+00', 'f47ac10b-58cc-4372-a567-0e02b2c3d479')
ORDER BY created_at DESC, id DESC
LIMIT 51; -- Fetches limit + 1 to detect has_next_page
```

1. **Cursor Extraction**:
   - Client sends: `?limit=50&cursor=MjAyNi0xMC0yOFQwOTowMDowMCswMDowMHxmNDdhYzEwYi01OGNjLTQzNzItYTU2Ny0wZTAyYjJjM2Q0Nzk`
   - `app/schemas/base.py: decode_cursor()` decodes Base64 into `(cursor_dt, cursor_id)`.
2. **Versioned Cache Check**:
   - Evaluates key: `cache:users:list:v{list_ver}:{filters_hash}`.
   - If hit, returns cached page directly from Redis.
3. **Database Keyset Seek**:
   - If miss, traverses PostgreSQL B-Tree composite index `ix_users_created_at_id` in $O(\log N)$ time.
   - Fetches 51 records. If 51 returned, `has_next = True`, and the 51st record is trimmed.
4. **Next Cursor Generation**:
   - `app/schemas/base.py: encode_cursor(last_item.created_at, last_item.id)` encodes the new cursor token.
5. **Cache Store**:
   - Caches the page dictionary in Redis under the current list version.

---

## 1.4 Elasticsearch Search & Async Logging Lifecycle

The architecture implements **Command-Query Responsibility Segregation (CQRS)**: PostgreSQL is the authoritative system of record for all writes, while Elasticsearch 8 handles full-text fuzzy ranking, faceted search, and centralized high-throughput structured application logging.

### CQRS Product Full-Text Search Path: `GET /api/v1/products/search?q=wireless`

```
[Client] ──► GET /api/v1/products/search?q=wireless&limit=20
   │
   ▼
[app/api/crud_router.py: CRUDRouter.search()]
   │ Extracts 'q', pagination parameters, and validates
   ▼
[app/services/product.py: ProductService.search_products(q="wireless", limit=20)]
   │
   ▼
[app/core/elasticsearch.py: ElasticsearchService.search_products(query="wireless")]
   │ Constructs Elasticsearch DSL:
   │ {
   │   "query": {
   │     "multi_match": {
   │       "query": "wireless",
   │       "fields": ["name^3", "description^2", "category", "sku"],
   │       "fuzziness": "AUTO"
   │     }
   │   },
   │   "size": 20
   │ }
   │
   ├─► [Elasticsearch Cluster Active (HTTP 200)]
   │   ├─ Extracts hits._source documents
   │   └─ Deserializes into List[ProductRead] in < 5ms
   │
   └─► [Elasticsearch Cluster Down or Disabled (Graceful Degradation)]
       ├─ NullElasticsearch catches connection errors / circuit breaks
       ├─ Emits warning log
       └─ Degrades gracefully to empty list or fallback database ILIKE query
   ▼
[Fast Serialization via ORJSONResponse ──► Client receives HTTP 200 OK]
```

### High-Throughput Non-Blocking Structured Log Ingestion

To serve **100,000 concurrent requests** without disk I/O bottlenecks or Elasticsearch latency impacting user responses, log events are processed via a decoupled, bounded in-memory buffer:

```
[Incoming Request] ──► request_id_and_tracing_middleware()
   │ Injects contextvars: request_id, trace_id, client_ip
   ▼
[Application Layer Log] ──► logger.info("Order created", extra={"order_id": str(id)})
   │
   ▼
[StructuredLogFormatter: app/core/logging.py]
   │ ├─ Formats JSON payload adhering to Elastic Common Schema (ECS)
   │ ├─ Injects contextvars: request_id, trace_id, environment
   │ └─ Sanitizes PII (masks credit cards, passwords, authorization headers)
   ▼
[AsyncLogBuffer: Non-Blocking Bounded Queue (size=10,000)]
   │ queue.put_nowait(log_dict) ──► Returns IMMEDIATELY in 0.001ms!
   │ If queue full (extreme spike) ──► Drops item to protect ASGI event loop
   ▼
[Background Worker Task: app/core/logging.py]
   │ Flushes every 1.0s OR when batch reaches 250 records
   ▼
[Elasticsearch 8 Bulk API: _bulk]
   │ await es_client.bulk(operations=bulk_operations)
   │ Writes to daily rollover index: app-logs-YYYY.MM.DD
   ▼
[Searchable via GET /api/v1/logs/search?level=ERROR&time_range=24h]
```

---

## 1.5 Relational & Nested Query Execution Lifecycle

When querying complex nested structures (e.g. a `Product` and its associated `Category`):

### The Async N+1 Dilemma & `lazy="raise"`
In traditional synchronous frameworks (like Django or sync SQLAlchemy), accessing `product.category_rel.name` automatically issues a hidden synchronous query behind the scenes. In an async event-loop architecture serving 100k concurrency, **implicit lazy loading blocks the entire async thread worker** and crashes with:
```
sqlalchemy.exc.MissingGreenlet: greenlet_spawn has not been called; can't call await_only() using greenlet_spawn
```
To eliminate this hazard permanently, all relationships in this codebase declare `lazy="raise"`. Every relational attribute access that is not explicitly pre-loaded immediately raises an error during testing.

### Eager Loaded Nested Path: `GET /api/v1/products/{id}/detail`

```
[Client] ──► GET /api/v1/products/f47ac10b-58cc-4372-a567-0e02b2c3d479/detail
   │
   ▼
[app/api/v1/products.py: router.get("/{item_id}/detail")]
   │ Endpoint calls: await service.get_detail(item_id)
   ▼
[app/services/product.py: ProductService.get_detail()]
   │ Calls: await self.repository.get_with_category(product_id)
   ▼
[app/repositories/product.py: ProductRepository.get_with_category()]
   │ Executes:
   │ await self.get(product_id, options=[selectinload(Product.category_rel)])
   ▼
[SQLAlchemy Async Engine & PgBouncer Multiplexer]
   │ Issues EXACTLY 2 optimized queries:
   │ 1. SELECT * FROM products WHERE id = 'f47ac10b-...'
   │ 2. SELECT * FROM categories WHERE id IN (product.category_id)
   ▼
[Entities Linked in Identity Map: product.category_rel is fully populated]
   │
   ▼
[Pydantic v2 DTO Serialization: ProductDetail.model_validate(product)]
   │ Schema nests CategoryRead inside ProductDetail:
   │ {
   │   "id": "f47ac10b-58cc-4372-a567-0e02b2c3d479",
   │   "name": "Wireless Mechanical Keyboard",
   │   "sku": "KEY-WL-01",
   │   "price_cents": 8999,
   │   "category_id": 1,
   │   "category_rel": {
   │     "id": 1,
   │     "name": "Electronics",
   │     "description": "Electronic components and accessories",
   │     "is_active": true
   │   }
   │ }
   ▼
[Fast Serialization via ORJSONResponse ──► Client receives HTTP 200 OK]
```

---

# Part 2: Developer Implementation Workflow

This section is a complete, copy-pasteable reference for developers creating a new entity, CRUD operations, or custom routes in the project.

---

## 2.1 File Modification Checklist

When adding a new entity (for example, `Order`), follow this checklist:

| Action | File Path | Purpose |
|---|---|---|
| **CREATE** | [`app/models/order.py`](file:///home/nazmul/Downloads/fastapi-repo-pattern/app/models/order.py) | SQLAlchemy 2.0 Declarative Table Model |
| **CREATE** | [`app/schemas/order.py`](file:///home/nazmul/Downloads/fastapi-repo-pattern/app/schemas/order.py) | Pydantic v2 Schemas (`OrderCreate`, `OrderUpdate`, `OrderRead`, `OrderPage`) |
| **CREATE** | [`app/repositories/order_repository.py`](file:///home/nazmul/Downloads/fastapi-repo-pattern/app/repositories/order_repository.py) | Subclass of `BaseRepository[Order, uuid.UUID]` |
| **CREATE** | [`app/services/order_service.py`](file:///home/nazmul/Downloads/fastapi-repo-pattern/app/services/order_service.py) | Subclass of `BaseService` with domain logic and hooks |
| **CREATE** | [`app/api/v1/orders.py`](file:///home/nazmul/Downloads/fastapi-repo-pattern/app/api/v1/orders.py) | Dynamic `CRUDRouter` with zero explicit boilerplate |
| **CREATE** | [`tests/test_orders.py`](file:///home/nazmul/Downloads/fastapi-repo-pattern/tests/test_orders.py) | Automated tests (CRUD, pagination, validation, 404s) |
| **MODIFY** | [`app/models/__init__.py`](file:///home/nazmul/Downloads/fastapi-repo-pattern/app/models/__init__.py) | Export `Order` model for discovery |
| **MODIFY** | [`app/schemas/__init__.py`](file:///home/nazmul/Downloads/fastapi-repo-pattern/app/schemas/__init__.py) | Re-export schemas |
| **MODIFY** | [`app/repositories/__init__.py`](file:///home/nazmul/Downloads/fastapi-repo-pattern/app/repositories/__init__.py) | Re-export repository |
| **MODIFY** | [`app/services/__init__.py`](file:///home/nazmul/Downloads/fastapi-repo-pattern/app/services/__init__.py) | Re-export service |
| **MODIFY** | [`app/api/v1/router.py`](file:///home/nazmul/Downloads/fastapi-repo-pattern/app/api/v1/router.py) | Mount `orders.router` under `/orders` |

---

## 2.2 In-Depth Step-by-Step Code Examples

### Step 1: Create the Model (`app/models/order.py`)

```python
"""Order database entity with keyset pagination composite indexing."""

from datetime import datetime
import uuid

from sqlalchemy import DateTime, Index, Integer, String, func
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.models.base import Base


class Order(Base):
    __tablename__ = "orders"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
    )
    user_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        index=True,
        nullable=False,
    )
    total_cents: Mapped[int] = mapped_column(Integer, nullable=False)
    status: Mapped[str] = mapped_column(
        String(50),
        default="pending",
        index=True,
        nullable=False,
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )

    # MANDATORY: Composite B-Tree index for O(log N) Keyset Pagination
    __table_args__ = (
        Index("ix_orders_created_at_id", "created_at", "id"),
    )
```

**Key Requirements**:
1. Inherit from `app.models.base.Base`.
2. Always define the composite index: `Index("ix_<table_name>_created_at_id", "created_at", "id")`.
3. Use `UUID(as_uuid=True)` for native PostgreSQL UUIDs.

---

### Step 2: Create Schemas (`app/schemas/order.py`)

```python
"""Pydantic v2 schemas for Order validation and serialization."""

from datetime import datetime
from typing import List, Optional
import uuid

from pydantic import BaseModel, ConfigDict, Field


class OrderCreate(BaseModel):
    user_id: uuid.UUID
    total_cents: int = Field(gt=0, description="Order total in cents (must be positive)")
    status: str = Field(default="pending", max_length=50)


class OrderUpdate(BaseModel):
    status: Optional[str] = Field(default=None, max_length=50)
    total_cents: Optional[int] = Field(default=None, gt=0)


class OrderRead(BaseModel):
    id: uuid.UUID
    user_id: uuid.UUID
    total_cents: int
    status: str
    created_at: datetime
    updated_at: datetime

    model_config = ConfigDict(from_attributes=True)


class OrderPage(BaseModel):
    items: List[OrderRead]
    next_cursor: Optional[str] = None
    limit: int = 50
```

---

### Step 3: Create Repository (`app/repositories/order_repository.py`)

```python
"""Order repository inheriting generic CRUD and keyset pagination primitives."""

from typing import List, Optional
import uuid

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.order import Order
from app.repositories.base import BaseRepository


class OrderRepository(BaseRepository[Order, uuid.UUID]):
    def __init__(self, session: AsyncSession) -> None:
        super().__init__(Order, session)

    # -------------------------------------------------------------------------
    # Inherited automatically from BaseRepository:
    # - get(id)
    # - get_by(**kwargs)
    # - exists_by(**kwargs)
    # - create(entity)
    # - update(entity, data)
    # - delete(entity)
    # - list_keyset(limit, cursor_dt, cursor_id, **filters)
    # -------------------------------------------------------------------------

    # Custom repository query example:
    async def list_by_user(self, user_id: uuid.UUID, limit: int = 100) -> List[Order]:
        """Custom query fetching recent orders for a specific customer."""
        stmt = (
            select(Order)
            .where(Order.user_id == user_id)
            .order_by(Order.created_at.desc())
            .limit(limit)
        )
        result = await self.session.execute(stmt)
        return list(result.scalars().all())
```

---

### Step 4: Create Service (`app/services/order_service.py`)

```python
"""Order domain service with caching and lifecycle hooks."""

from typing import Optional
import uuid

from app.core.exceptions import ConflictError, NotFoundError
from app.models.order import Order
from app.repositories.order_repository import OrderRepository
from app.schemas.order import OrderCreate, OrderPage, OrderRead, OrderUpdate
from app.services.base import BaseService, DynamicRedisCache


class OrderService(BaseService[Order, OrderCreate, OrderUpdate, OrderRead, uuid.UUID]):
    def __init__(
        self,
        repo: OrderRepository,
        cache: Optional[DynamicRedisCache] = None,
    ) -> None:
        super().__init__(repo=repo, read_schema_cls=OrderRead, cache=cache)

    # -------------------------------------------------------------------------
    # Lifecycle Hooks
    # -------------------------------------------------------------------------
    async def before_create(self, payload: OrderCreate) -> Order:
        """Called before repository insertion. Generate IDs, validate business rules."""
        return Order(
            id=uuid.uuid4(),
            user_id=payload.user_id,
            total_cents=payload.total_cents,
            status=payload.status,
        )

    async def before_update(self, entity: Order, payload: OrderUpdate) -> None:
        """Called before database flush. Apply partial updates."""
        update_data = payload.model_dump(exclude_unset=True)
        for field, value in update_data.items():
            setattr(entity, field, value)

    # -------------------------------------------------------------------------
    # Custom Domain Methods
    # -------------------------------------------------------------------------
    async def list_orders(
        self,
        limit: int = 50,
        cursor: Optional[str] = None,
        user_id: Optional[uuid.UUID] = None,
        cache_enabled: Optional[bool] = None,
    ) -> OrderPage:
        """Keyset paginated listing with optional user_id filter."""
        filters = {}
        if user_id:
            filters["user_id"] = user_id

        page_dict = await self.list_keyset(
            limit=limit,
            cursor=cursor,
            cache_enabled=cache_enabled,
            **filters,
        )
        return OrderPage(**page_dict)

    async def cancel_order(self, order_id: uuid.UUID) -> OrderRead:
        """Domain action: transitions order status to 'cancelled'."""
        order = await self.repo.get(order_id)
        if not order:
            raise NotFoundError(f"Order {order_id} not found")

        if order.status == "completed":
            raise ConflictError("Cannot cancel an already completed order")

        order.status = "cancelled"
        updated_order = await self.repo.update(order, data={"status": "cancelled"})

        # Invalidate cache for this item and versioned lists
        if self.cache:
            await self.cache.delete_item(order_id)
            await self.cache.invalidate_lists()

        return await self.to_read_schema(updated_order)
```

---

### Step 5: Dynamic Router (`app/api/v1/orders.py`)

Using `CRUDRouter`, standard CRUD (`POST /`, `GET /`, `GET /{item_id}`, `PUT /{item_id}`, `PATCH /{item_id}`, `DELETE /{item_id}`, `GET /search`) with keyset cursor pagination, dynamic Redis Cluster caching, and validation is generated dynamically with zero boilerplate. Custom domain endpoints are attached directly to the same router:

```python
"""Dynamic CRUD router for Orders with zero explicit CRUD handler boilerplate."""

from typing import Annotated
import uuid

from fastapi import Depends

from app.api.crud_router import CRUDRouter
from app.api.deps import provide
from app.models.order import Order
from app.schemas.order import OrderCreate, OrderRead, OrderUpdate
from app.services.order_service import OrderService

# Dynamic dependency provider injecting AsyncSession, Redis Cluster, Crypto, and Elasticsearch
get_order_service = provide(OrderService, cache_prefix="orders")
OrderServiceDep = Annotated[OrderService, Depends(get_order_service)]

router = CRUDRouter(
    model=Order,
    service=get_order_service,
    create_schema=OrderCreate,
    update_schema=OrderUpdate,
    read_schema=OrderRead,
    prefix="/orders",
    tags=["Orders"],
    filter_fields={
        "user_id": ["eq"],
        "status": ["eq", "in"],
    },
    sort_fields=["id", "created_at", "total_cents"],
)


# Attach custom domain actions without duplicating generic CRUD handlers
@router.post("/{item_id}/cancel", response_model=OrderRead, summary="Cancel order")
async def cancel_order(
    item_id: uuid.UUID,
    service: OrderServiceDep,
) -> OrderRead:
    """Cancels an existing order."""
    return await service.cancel_order(item_id)


__all__ = ["router", "get_order_service", "OrderServiceDep"]
```

---

### Step 6: Register the Route & Exports

1. **Mount in Master Router** ([`app/api/v1/router.py`](file:///home/nazmul/Downloads/fastapi-repo-pattern/app/api/v1/router.py)):
   ```python
   from app.api.v1.categories import router as categories_router
   from app.api.v1.health import router as health_router
   from app.api.v1.orders import router as orders_router
   from app.api.v1.products import router as products_router
   from app.api.v1.users import router as users_router

   api_router = APIRouter()
   api_router.include_router(users_router)
   api_router.include_router(products_router)
   api_router.include_router(categories_router)
   api_router.include_router(orders_router)  # <-- Mounts /orders CRUD + custom actions
   api_router.include_router(health_router)
   ```

2. **Re-export in Package Inits**:
   - `app/models/__init__.py`: Add `from app.models.order import Order`
   - `app/schemas/__init__.py`: Add `from app.schemas.order import OrderCreate, OrderRead, OrderUpdate, OrderPage`
   - `app/repositories/__init__.py`: Add `from app.repositories.order_repository import OrderRepository`
   - `app/services/__init__.py`: Add `from app.services.order_service import OrderService`

---

## 2.3 Unit of Work & Multi-Repository Transactions

If a business action must modify multiple entities atomically (for example, creating an order and reducing inventory):

### Pattern A: Shared AsyncSession via `get_db`
```python
class CheckoutService:
    def __init__(self, order_repo: OrderRepository, product_repo: ProductRepository):
        self.order_repo = order_repo
        self.product_repo = product_repo

    async def checkout(self, user_id: uuid.UUID, sku: str, quantity: int) -> OrderRead:
        # Both repositories share the SAME AsyncSession injected by get_db!
        product = await self.product_repo.get_by(sku=sku)
        if not product:
            raise NotFoundError("Product not found")

        # Create Order
        order = await self.order_repo.create(Order(user_id=user_id, total_cents=product.price_cents * quantity))

        # Both changes were flushed (not committed)
        # When get_db exits, it executes ONE single commit for both tables atomically!
        return OrderRead.model_validate(order)
```

### Pattern B: Multi-Repository Atomic Transaction with Shared Session
```python
async def process_checkout(db: AsyncSession, user_id: uuid.UUID, sku: str, quantity: int):
    product_repo = ProductRepository(db)
    order_repo = OrderRepository(db)

    product = await product_repo.get_by(sku=sku)
    if not product:
        raise NotFoundError("Product not found")

    order = await order_repo.create(
        Order(user_id=user_id, total_cents=product.price_cents * quantity)
    )
    await product_repo.update(product, data={"stock_count": product.stock_count - quantity})
    await db.commit()  # Atomic single commit across all repositories
```

### Dynamic Operator Query Engine
Every repository extending [`BaseRepository`](file:///home/nazmul/Downloads/fastapi-repo-pattern/app/repositories/base.py) inherits the dynamic filter engine:
```python
# Operator Lookups (Double Underscore):
# - `field__eq`: Equality (`col == val`)
# - `field__ne`: Inequality (`col != val`)
# - `field__gt`, `field__gte`: Greater than / Greater than or equal (`col >= val`)
# - `field__lt`, `field__lte`: Less than / Less than or equal (`col <= val`)
# - `field__in`, `field__notin`: Set membership (`col.in_(val)`, `~col.in_(val)`)
# - `field__like`, `field__ilike`: SQL LIKE / ILIKE (`col.ilike(val)`)
# - `field__contains`, `field__icontains`: Substring match (`col.contains(val)`)
# - `field__startswith`, `field__endswith`: Prefix/Suffix match
# - `field__isnull`: Null check (`col.is_(None)` if True else `col.isnot(None)`)

# Example:
results = await repo.list(
    total_cents__gte=1000,
    status__in=["pending", "processing"],
    order_by=["-created_at", "total_cents"],
)
```

**Golden Rules**:
- Never call `session.commit()` inside repositories or services.
- Repositories call `session.flush()`.
- `get_db` or `uow` manages `commit()` and `rollback()` at the transaction boundary.


---

## 2.4 Writing Automated Tests (`tests/test_orders.py`)

Every new entity should have tests validating CRUD, caching, and error paths:

```python
import pytest
from app.core.exceptions import NotFoundError
from app.models.order import Order
from app.repositories.order_repository import OrderRepository
from app.schemas.order import OrderCreate, OrderRead, OrderUpdate
from app.services.order_service import OrderService

@pytest.mark.asyncio
async def test_order_crud_lifecycle(async_session, mock_redis):
    repo = OrderRepository(async_session)
    service = OrderService(repo=repo, cache=mock_redis)

    # 1. Create
    payload = OrderCreate(user_id=uuid.uuid4(), total_cents=5000, status="pending")
    order = await service.create(payload)
    assert order.total_cents == 5000
    assert order.status == "pending"

    # 2. Read with Cache
    cached = await service.get_by_id(order.id)
    assert cached.id == order.id

    # 3. 404 Verification
    with pytest.raises(NotFoundError):
        await service.get_by_id(uuid.uuid4())
```

---

## 2.5 Nested Tables & Entity Relationships Guide (Category in Product)

This section details how to model, query, validate, cache, and migrate nested tables and relationships in this architecture, using the **Category $\leftrightarrow$ Product** relationship as the canonical production reference.

<p align="center">
  <img src="assets/relational_nested_flow.png" alt="Relational Eager Loading Pipeline" width="100%" />
</p>

---

### 2.5.1 The `lazy="raise"` Rule in Async SQLAlchemy

In an asynchronous high-concurrency architecture serving **100,000 concurrent requests**, implicit lazy loading is strictly prohibited:

1. **The Event Loop Trap**: In synchronous SQLAlchemy, accessing `product.category_rel` triggers a blocking synchronous I/O query. In an `asyncio` event loop, this blocks the worker process or raises `sqlalchemy.exc.MissingGreenlet`.
2. **The $N+1$ Database Penalty**: In a list of 100 products, naive attribute access executes $1 + 100 = 101$ roundtrips to the database multiplexer, exhausting PgBouncer backend connections instantly.
3. **The Architectural Fix**: All relationship attributes in this repository must declare `lazy="raise"`. If a developer forgets to eager-load a relationship in the repository, SQLAlchemy immediately raises an exception at the query boundary rather than silently leaking queries into production.

---

### 2.5.2 Step 1: Model Definition with Foreign Keys & Relationships

Both sides of the relationship are mapped using SQLAlchemy 2.0 type-annotated columns:

#### Parent Entity: `Category` ([`app/models/category.py`](file:///home/nazmul/Downloads/fastapi-repo-pattern/app/models/category.py))
```python
from typing import List, Optional, TYPE_CHECKING
from sqlalchemy import Boolean, String, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship
from app.db.base import Base, IdIntMixin, TimestampMixin

if TYPE_CHECKING:
    from app.models.product import Product

class Category(IdIntMixin, TimestampMixin, Base):
    __tablename__ = "categories"

    name: Mapped[str] = mapped_column(String(100), unique=True, index=True, nullable=False)
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True, default=None)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)

    # 1:N Relationship: one category has many products
    # lazy="raise" prevents accidental N+1 queries
    products: Mapped[List["Product"]] = relationship(
        "Product",
        back_populates="category_rel",
        lazy="raise",
    )
```

#### Child Entity: `Product` ([`app/models/product.py`](file:///home/nazmul/Downloads/fastapi-repo-pattern/app/models/product.py))
```python
from typing import Optional, TYPE_CHECKING
import uuid
from sqlalchemy import Boolean, DateTime, ForeignKey, Index, Integer, String, func
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship
from app.models.base import Base

if TYPE_CHECKING:
    from app.models.category import Category

class Product(Base):
    __tablename__ = "products"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    sku: Mapped[str] = mapped_column(String(100), unique=True, index=True, nullable=False)
    price_cents: Mapped[int] = mapped_column(Integer, nullable=False)

    # 1. Foreign Key with ondelete="RESTRICT"
    # RESTRICT blocks deleting a Category if products are assigned to it (raises 409 Conflict)
    category_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("categories.id", ondelete="RESTRICT"),
        nullable=True,
        index=True,  # Crucial: Always index foreign keys for join performance
    )

    # 2. Bidirectional Relationship with lazy="raise"
    category_rel: Mapped[Optional["Category"]] = relationship(
        "Category",
        back_populates="products",
        lazy="raise",
    )
```

> [!IMPORTANT]
> - Always place `index=True` on Foreign Key columns. Without a B-Tree index on the foreign key, PostgreSQL must execute a full-table scan on the child table whenever the parent table is queried or modified.
> - Use `ondelete="RESTRICT"` for critical business hierarchies to prevent accidental cascading deletion of inventory or orders.

---

### 2.5.3 Step 2: Nested Pydantic Schemas (Embedded DTOs & Validation)

To optimize payload sizes, separate standard flat CRUD schemas from enriched nested detail schemas:

#### Schema Definitions ([`app/schemas/product.py`](file:///home/nazmul/Downloads/fastapi-repo-pattern/app/schemas/product.py)):
```python
from datetime import datetime
from typing import Optional, Union
import uuid
from pydantic import BaseModel, ConfigDict, Field
from app.schemas.category import CategoryRead

# 1. Base Product Schema (Flat attributes)
class ProductBase(BaseModel):
    name: str = Field(..., min_length=1, max_length=255)
    sku: str = Field(..., min_length=1, max_length=100)
    price_cents: int = Field(..., ge=0)
    category_id: Optional[int] = Field(default=None, description="Foreign key to categories")
    is_active: bool = Field(default=True)
    stock: int = Field(default=0, ge=0)

# 2. Creation Payload (Client supplies category_id)
class ProductCreate(ProductBase):
    pass

# 3. Standard Flat Read Schema (Used for fast keyset pagination)
class ProductRead(BaseModel):
    id: Union[uuid.UUID, int]
    name: str
    sku: str
    price_cents: int
    category_id: Optional[int] = None
    is_active: bool
    stock: int = 0
    created_at: datetime
    updated_at: Optional[datetime] = None

    model_config = ConfigDict(from_attributes=True)

# 4. Nested Detail Schema (Eagerly embeds CategoryRead)
class ProductDetail(ProductRead):
    category_rel: Optional[CategoryRead] = None
```

---

### 2.5.4 Step 3: Repository Eager Loading (`selectinload` vs `joinedload`)

When querying entities with relationships, instruct SQLAlchemy to pre-load relations in the repository query:

#### When to use which loader?
- **`selectinload` (Recommended for 1:N collections and async N:1)**:
  Executes two queries linked by primary keys:
  ```sql
  SELECT * FROM products WHERE id = :id;
  SELECT * FROM categories WHERE id IN (:category_id);
  ```
  Prevents Cartesian product duplicates across tables.
- **`joinedload` (Recommended for strict 1:1 foreign keys)**:
  Uses an SQL `LEFT OUTER JOIN`. Do not use `joinedload` on large collections with `limit` or `offset`, as SQL joins multiply row counts before slicing.

#### Implementation in [`app/repositories/product.py`](file:///home/nazmul/Downloads/fastapi-repo-pattern/app/repositories/product.py):
```python
from sqlalchemy.orm import selectinload
from app.models.product import Product
from app.repositories.base import BaseRepository

class ProductRepository(BaseRepository[Product, uuid.UUID]):
    def __init__(self, db: AsyncSession) -> None:
        super().__init__(model=Product, db=db)

    async def get_with_category(self, product_id: Union[uuid.UUID, int, str]) -> Optional[Product]:
        """Eagerly loads product with its associated category using selectinload."""
        return await self.get(
            product_id,
            options=[selectinload(Product.category_rel)],
        )

    async def list_with_category(self, limit: int = 50, **filters) -> Sequence[Product]:
        """Lists products eagerly loading the category relation."""
        return await self.list(
            limit=limit,
            options=[selectinload(Product.category_rel)],
            **filters,
        )
```

---

### 2.5.5 Step 4: Service Layer Validation & Cross-Entity Cache Invalidation

The Service layer enforces cross-entity business rules and cache consistency across parent and child entities:

#### Implementation in [`app/services/product.py`](file:///home/nazmul/Downloads/fastapi-repo-pattern/app/services/product.py):
```python
from app.core.exceptions import BusinessRuleError, NotFoundError
from app.repositories.category import CategoryRepository
from app.schemas.product import ProductCreate, ProductDetail, ProductRead, ProductUpdate
from app.services.base import BaseService

class ProductService(BaseService[Product, ProductCreate, ProductUpdate, ProductRead, uuid.UUID]):

    async def _ensure_active_category(self, category_id: int) -> None:
        """Validates that the referenced category exists and is currently active."""
        if self.db is not None:
            categories = CategoryRepository(self.db)
            if not await categories.exists(id=category_id, is_active=True):
                raise BusinessRuleError(f"Category id={category_id} does not exist or is inactive.")

    async def before_create(self, payload: ProductCreate) -> Product:
        """Lifecycle Hook: Enforces active category validation before saving."""
        if getattr(payload, "category_id", None) is not None:
            await self._ensure_active_category(payload.category_id)
        return await super().before_create(payload)

    async def before_update(self, entity: Product, payload: ProductUpdate) -> None:
        """Lifecycle Hook: Validates category if modified during update."""
        if getattr(payload, "category_id", None) is not None and payload.category_id != entity.category_id:
            await self._ensure_active_category(payload.category_id)
        await super().before_update(entity, payload)

    async def get_detail(self, product_id: Union[uuid.UUID, int, str]) -> ProductDetail:
        """Queries product with eagerly loaded category relation."""
        product = await self.repository.get_with_category(product_id)
        if product is None:
            raise NotFoundError(f"Product with id={product_id} not found.")
        return ProductDetail.model_validate(product)
```

#### Cache Coordination Between Parent and Child:
When a parent `Category` is updated or deactivated:
```python
# Inside CategoryService.update_by_id():
updated_category = await self.repo.update(...)

# Invalidate product list version cache so all product listings re-fetch updated category data:
await self.redis.incr("cache:products:list_ver")
```

---

### 2.5.6 Step 5: Dynamic CRUDRouter & Custom Nested Endpoints

In [`app/api/v1/products.py`](file:///home/nazmul/Downloads/fastapi-repo-pattern/app/api/v1/products.py), standard endpoints are generated by `CRUDRouter`, while relational query filters and detailed nested endpoints are configured with zero boilerplate:

```python
from app.api.crud_router import CRUDRouter
from app.api.deps import ProductServiceDep, get_product_service
from app.models.product import Product
from app.schemas.product import ProductCreate, ProductDetail, ProductRead, ProductUpdate

# 1. Register category_id as a queryable filter
router = CRUDRouter(
    model=Product,
    service=get_product_service,
    create_schema=ProductCreate,
    update_schema=ProductUpdate,
    read_schema=ProductRead,
    prefix="/products",
    tags=["Products"],
    filter_fields={
        "category_id": ["eq", "in"],  # Enables ?category_id=1 and ?category_id__in=1,2,3
        "is_active": ["eq"],
    },
    sort_fields=["id", "name", "price_cents", "created_at"],
)

# 2. Attach Custom Relational Detail Endpoint
@router.get("/{item_id}/detail", response_model=ProductDetail, summary="Product with its category")
async def product_detail(item_id: Union[uuid.UUID, int], service: ProductServiceDep):
    """Returns the full product entity with nested category details."""
    return await service.get_detail(item_id)
```

---

### 2.5.7 Step 6: Database Migrations with Alembic

Whenever foreign keys or relationship tables are added or modified:

1. **Generate the Migration Script**:
   ```bash
   alembic revision --autogenerate -m "add_category_fk_to_products"
   ```

2. **Inspect the Generated Migration File**:
   Verify that the migration correctly includes:
   ```python
   # Inside alembic/versions/<hash>_add_category_fk_to_products.py
   def upgrade() -> None:
       op.add_column('products', sa.Column('category_id', sa.Integer(), nullable=True))
       op.create_index('ix_products_category_id', 'products', ['category_id'], unique=False)
       op.create_foreign_key(
           'fk_products_category_id_categories',
           'products',
           'categories',
           ['category_id'],
           ['id'],
           ondelete='RESTRICT'
       )

   def downgrade() -> None:
       op.drop_constraint('fk_products_category_id_categories', 'products', type_='foreignkey')
       op.drop_index('ix_products_category_id', table_name='products')
       op.drop_column('products', 'category_id')
   ```

3. **Verify Schema Health**:
   Run the migration helper script:
   ```bash
   python3 .agents/agents/developer-agent/skills/db_migration_helper.py
   ```

4. **Apply Migration to Database**:
   ```bash
   alembic upgrade head
   ```

---

### 2.5.8 Step 7: Testing Relationships & Preventing N+1 Regressions

Add automated test cases validating foreign key constraints and eager loading ([`tests/test_products.py`](file:///home/nazmul/Downloads/fastapi-repo-pattern/tests/test_products.py)):

```python
import pytest
from app.core.exceptions import BusinessRuleError
from app.models.category import Category
from app.models.product import Product
from app.repositories.product import ProductRepository
from app.schemas.product import ProductCreate, ProductDetail
from app.services.product import ProductService

@pytest.mark.asyncio
async def test_product_with_category_eager_loading(async_session, mock_redis):
    # 1. Create Parent Category
    category = Category(name="Electronics", is_active=True)
    async_session.add(category)
    await async_session.flush()

    # 2. Create Child Product
    repo = ProductRepository(async_session)
    service = ProductService(repo=repo, cache=mock_redis, db=async_session)
    payload = ProductCreate(
        name="Mechanical Keyboard",
        sku="KEY-001",
        price_cents=7999,
        category_id=category.id,
    )
    product = await service.create(payload)
    await async_session.commit()

    # 3. Verify Eager Loading via Detail Endpoint
    detail = await service.get_detail(product.id)
    assert isinstance(detail, ProductDetail)
    assert detail.category_rel is not None
    assert detail.category_rel.name == "Electronics"

@pytest.mark.asyncio
async def test_product_category_inactive_rejected(async_session, mock_redis):
    # Create inactive category
    category = Category(name="Discontinued", is_active=False)
    async_session.add(category)
    await async_session.flush()

    repo = ProductRepository(async_session)
    service = ProductService(repo=repo, cache=mock_redis, db=async_session)
    payload = ProductCreate(
        name="Old Item",
        sku="OLD-001",
        price_cents=1000,
        category_id=category.id,
    )
    # Business rule must reject inactive categories
    with pytest.raises(BusinessRuleError):
        await service.create(payload)
```



---

# Part 3: High-Performance Infrastructure & State Engines (Redis, Elasticsearch & Apache Kafka)

This section provides an architectural deep dive into how **Redis 7 Cluster**, **Elasticsearch 8**, and **Apache Kafka** cooperate with PostgreSQL and PgBouncer to sustain **100,000 concurrent requests** with strict data integrity, sub-millisecond latencies, and zero cross-process shared state.

```
┌────────────────────────────────────────────────────────────────────────────────────────┐
│                                ARCHITECTURE TIERING & DIVISION                         │
├───────────────────┬───────────────────┬────────────────────────┬───────────────────────┤
│ Tier / Engine     │ Storage Physics   │ Concurrency Model      │ Primary Responsibility│
├───────────────────┼───────────────────┼────────────────────────┼───────────────────────┤
│ PostgreSQL 16     │ Disk + MVCC       │ PgBouncer Multiplexed  │ ACID Source of Truth  │
│ Redis 7 Cluster   │ In-Memory RAM     │ Atomic Non-Blocking    │ Ephemeral State & Lock│
│ Elasticsearch 8   │ Lucene Inverted   │ Sharded Read Clustered │ CQRS Search & Logs    │
│ Apache Kafka      │ Append-Only Log   │ Partition Consumer Grp │ Event-Driven Streaming│
└───────────────────┴───────────────────┴────────────────────────┴───────────────────────┘
```

---

## 3.1 Redis 7 Cluster Architecture & Workflows

Redis operates exclusively in RAM with sub-millisecond $O(1)$ operations, making it the bedrock for distributed synchronization, rate limiting, and hot entity caching across horizontal worker instances.

### 3.1.1 Distributed Mutex Locking & Stampede Protection

#### The Cache Stampede Problem
Under 100,000 concurrent requests, when a hot cache key expires (e.g. `cache:product:xyz`), hundreds of worker processes simultaneously query the PostgreSQL database. This causes a **Cache Stampede (Dogpile Effect)**, exhausting database connections on PgBouncer and causing cascading timeouts.

#### The Distributed Lock Implementation ([`app/core/redis.py`](file:///home/nazmul/Downloads/fastapi-repo-pattern/app/core/redis.py))
To prevent this, the application uses an atomic Redis mutex with Lua script release verification:

```python
class DistributedLock:
    def __init__(self, redis: Union[RedisCluster, Redis]) -> None:
        self.redis = redis

    async def acquire(self, lock_key: str, token: str, ttl_ms: int = 5000) -> bool:
        # Atomic SET NX PX: Acquire lock only if key does not exist
        return bool(await self.redis.set(lock_key, token, px=ttl_ms, nx=True))

    async def release(self, lock_key: str, token: str) -> bool:
        # Atomic Lua Script: Releases lock ONLY if the token matches the owner
        # Prevents a slow worker from releasing a lock acquired by a newer worker
        RELEASE_LOCK_LUA = """
        if redis.call("get", KEYS[1]) == ARGV[1] then
            return redis.call("del", KEYS[1])
        else
            return 0
        end
        """
        return bool(await self.redis.eval(RELEASE_LOCK_LUA, 1, lock_key, token))
```

#### Stampede Protection Flow
1. Worker A encounters a cache miss on `cache:product:{id}`.
2. Worker A acquires mutex `lock:product:{id}` for 5000ms.
3. Workers B through Z fail to acquire the lock and wait (`asyncio.sleep(0.05)`).
4. Worker A queries PostgreSQL, populates `cache:product:{id}`, and releases the lock.
5. Workers B through Z wake up, read the repopulated cache key directly from Redis in <1ms, and never touch the database.

---

### 3.1.2 Sub-Millisecond Point Lookups & Hot Caching

- **Key Format**: `cache:{entity_name}:{id}` (e.g., `cache:user:f81d4fae-7dec-11d0-a765-00a0c91e6bf6`).
- **Serialization**: Binary JSON encoded with `orjson` for high-throughput zero-copy deserialization.
- **Granular TTL Profiles**: Configured in [`app/core/config.py`](file:///home/nazmul/Downloads/fastapi-repo-pattern/app/core/config.py):
  - `CACHE_TTL_USER`: 300s (User profile records)
  - `CACHE_TTL_PRODUCT`: 600s (Catalog items)
  - `CACHE_TTL_REFERENCE`: 3600s (Static lookup tables, categories)
  - `CACHE_TTL_CONFIG`: 1800s (Feature flags, dynamic settings)

---

### 3.1.3 Dynamic Namespace Versioning ($O(1)$ Invalidation)

#### Why Mass Deletion Crashes Production
In high-throughput systems, scanning and deleting thousands of query keys (`DEL cache:user:list:*` or `KEYS cache:*`) locks the single-threaded Redis engine and kills performance.

#### The Namespace Versioning Solution ([`app/services/base.py`](file:///home/nazmul/Downloads/fastapi-repo-pattern/app/services/base.py))
The application maintains a version integer for each collection namespace:
```text
Redis Key: ns:user:version -> 42
Query Cache Key: cache:user:v42:limit=50&role=admin
```

When any mutation occurs (`POST /users`, `PATCH /users/{id}`, `DELETE /users/{id}`):
```python
# Atomic increment of namespace version key
await redis.incr(f"ns:{entity_name}:version")
```
- A single integer increment in Redis takes **microseconds**.
- All existing query cache keys for version 42 become inaccessible immediately.
- The next list queries automatically generate keys under version 43 (`cache:user:v43:...`).
- The obsolete version 42 keys expire safely according to their standard TTL (`CACHE_LIST_TTL=120s`) without wildcard scanning.

---

### 3.1.4 Request Idempotency Engine

Network dropouts between clients and reverse proxies can cause clients to retry write mutations (payment, order creation, reservations).
- **Key Format**: `idempotency:{key}`
- **TTL**: 86,400s (24 hours)
- **Lifecycle**:
  1. Client sends header `X-Idempotency-Key: <UUID>`.
  2. First request executes `SET idempotency:<UUID> PENDING NX EX 86400`.
  3. If acquisition succeeds: Request executes business logic, then stores final JSON response in `idempotency:<UUID>`.
  4. If acquisition fails: Returns existing cached response or HTTP 409 Conflict if still `PENDING`.

---

### 3.1.5 Verification Challenges & One-Time Tokens

For SMS verification codes, email magic links, or password reset challenges:
- **Key Format**: `challenge:{scope}:{token}` (e.g. `challenge:sms_verify:928374`)
- **TTL**: 300s (5 minutes)
- **One-Time Consumption**: Handled via single-roundtrip extraction (`GET` followed by atomic `DEL`). Storing transient tokens in PostgreSQL would cause excessive table bloat, autovacuum overhead, and write WAL amplification.

---

### 3.1.6 Sliding-Window Rate Limiting & Atomic Quotas

- **Engine**: [`app/core/rate_limit.py`](file:///home/nazmul/Downloads/fastapi-repo-pattern/app/core/rate_limit.py)
- **Key Format**: `rl:{client_ip}:{window_timestamp}`
- **Execution**: Atomic `INCRBY` + `EXPIRE` executed on request ingress.
- **Fail-Open Resilience**: If Redis is momentarily unreachable, the rate limiter logs a warning and allows traffic through (`RATE_LIMIT_FAIL_OPEN=true`), protecting API availability.

---

### 3.1.7 Negative Caching Sentinel

Attackers querying non-existent IDs (e.g. `GET /users/00000000-0000-0000-0000-000000000000`) bypass standard item caching and force continuous database lookups.
- **Sentinel**: `__NOT_FOUND__` stored with short TTL (`CACHE_NEGATIVE_TTL=30s`).
- Subsequent requests for the missing ID are absorbed in memory by Redis without touching PostgreSQL.

---

### 3.1.8 Cluster High-Availability & Standalone Fallback

In [`app/core/redis.py`](file:///home/nazmul/Downloads/fastapi-repo-pattern/app/core/redis.py):
- Connects across 16,384 hash slots on Redis 7 Cluster nodes (`redis-1:7000`, `redis-2:7001`, `redis-3:7002`).
- **Resilience**: If Cluster mode is inactive (e.g. local single-node development or maintenance mode), it catches `RedisClusterException` and automatically falls back to standalone Redis on the first reachable node.

---

## 3.2 Elasticsearch 8 Search & Analytics Architecture & Workflows

Elasticsearch provides distributed inverted indexing, BM25 scoring, and columnar Doc Values, decoupling analytical and full-text searches from PostgreSQL.

### 3.2.1 CQRS Read-Model Inverted Indexing

```
[Write Mutation] ──► PostgreSQL (Normalized ACID) ──► Asynchronous Indexing ──► Elasticsearch (Denormalized CQRS)
                                                                                       │
[Search Query]   ◄─────────────────────────────────────────────────────────────────────┘
```

When an entity (User, Category, Product) is created or updated:
1. PostgreSQL executes relational integrity checks and foreign keys.
2. A sanitized read DTO is constructed.
3. The document is indexed into `{index_prefix}-{entity}` (e.g. `demo-products`) via [`app/core/elasticsearch.py`](file:///home/nazmul/Downloads/fastapi-repo-pattern/app/core/elasticsearch.py).
4. Full-text searches query Elasticsearch directly, bypassing the PostgreSQL connection pool.

---

### 3.2.2 BM25 Full-Text Relevance Scoring & Fuzzy Matching

- **BM25 Algorithm**: Evaluates term frequency and inverse document frequency for multi-word queries.
- **Multi-Field Boosting**: Query weights search matches across fields:
  ```json
  {
    "multi_match": {
      "query": "mechanical keyboard",
      "fields": ["name^3", "description^1", "sku^5"],
      "fuzziness": "AUTO"
    }
  }
  ```
- **Typo Tolerance**: Levenshtein distance handles user misspellings without expensive regexes.

---

### 3.2.3 Columnar Doc Values & Real-Time Faceted Aggregations

E-commerce catalogs require dynamic category counts and price histograms:
- Elasticsearch uses **Doc Values** (disk-based columnar storage) to calculate aggregations across millions of matching documents in milliseconds.
- Resolves counts for filters (e.g., *Categories: Keyboards (120), Mice (45)*) in the same query as the search results.

---

### 3.2.4 Deep Pagination via Keyset & `search_after`

Standard SQL `OFFSET 50000 LIMIT 20` requires database engines to scan and discard 50,000 rows.
- Elasticsearch uses `search_after` based on sort keys and document IDs:
  ```json
  {
    "size": 20,
    "sort": [{"created_at": "desc"}, {"_id": "asc"}],
    "search_after": [1730467200, "f81d4fae-7dec-11d0-a765-00a0c91e6bf6"]
  }
  ```
- Traverses millions of records with constant $O(1)$ retrieval cost.

---

### 3.2.5 High-Throughput Non-Blocking Structured Logging

- **Engine**: [`app/core/logging.py`](file:///home/nazmul/Downloads/fastapi-repo-pattern/app/core/logging.py) (`AsyncLogBuffer`, `AsyncElasticsearchLogHandler`)
- **Queue Mechanics**: Incoming log records are dispatched to a bounded in-memory ring buffer (capacity: 1000).
- **Batching & Flushing**: A background task flushes logs in bulk HTTP requests (`_bulk` API) when either condition is met:
  - 100 log records accumulate (`ELASTICSEARCH_LOG_BATCH_SIZE=100`).
  - 2.0 seconds elapse (`ELASTICSEARCH_LOG_FLUSH_INTERVAL=2.0`).
- **Backpressure Protection**: If Elasticsearch is congested and the buffer reaches 1000 records, debug logs are dropped first, ensuring HTTP request workers are never blocked.
- **Log Index Lifecycle**: Log records are routed to `app-logs` timeseries indices with automated 30-day retention pruning (`ELASTICSEARCH_LOG_RETENTION_DAYS=30`).

---

### 3.2.6 Log Search API & Histogram Aggregations

Structured logs can be inspected programmatically via [`app/services/log_search.py`](file:///home/nazmul/Downloads/fastapi-repo-pattern/app/services/log_search.py):
- Filter by `correlation_id`, `service`, `level` (ERROR, WARNING, INFO), and ISO 8601 timestamp range.
- Query aggregate error-rate histograms over time for operational observability.

---

## 3.3 Apache Kafka Microservices Event-Driven Streaming

Apache Kafka provides durable, distributed event streaming for inter-service communication, inventory reservation sagas, and asynchronous outbox dispatch.

```
[Domain Mutation] ──► PostgreSQL Transaction ──► [outbox_events Table]
                            │ (Atomic Commit)
                            ▼
               [Outbox Dispatcher Daemon] ──► Kafka Broker (`domain.events`)
                                                    │
                                                    ▼
                                      [Microservice Consumers]
```

---

### 3.3.1 The Dual-Write Problem & Transactional Outbox Pattern

#### The Dual-Write Risk
Directly publishing to Kafka inside an HTTP handler creates data inconsistencies:
- If PostgreSQL commits but Kafka fails, the event is permanently lost.
- If Kafka succeeds but PostgreSQL transaction rolls back, downstream services process a ghost event.

#### The Transactional Outbox Solution ([`app/services/outbox.py`](file:///home/nazmul/Downloads/fastapi-repo-pattern/app/services/outbox.py))
Every domain event is written to the `outbox_events` table **in the same PostgreSQL transaction** as the business mutation:
```python
async with session.begin():
    # 1. Mutate domain entity
    session.add(user)
    # 2. Add outbox record in same ACID transaction
    outbox_record = OutboxEvent(
        aggregate_type="user",
        aggregate_id=user.id,
        event_type="user.created.v1",
        payload={"id": str(user.id)},
    )
    session.add(outbox_record)
# Atomic Commit: Either both succeed or both fail
```

---

### 3.3.2 Asynchronous Outbox Dispatcher Daemon

- **Background Dispatcher**: Runs in the background polling `outbox_events` with status `PENDING`.
- **Partition Keying by `aggregate_id`**: Events are published to Kafka keyed by aggregate ID (e.g. `user_id` or `order_id`). Kafka guarantees **strict FIFO ordering** for all events belonging to the same aggregate within a partition.
- **Sequential Safety**: The dispatcher will not publish a later event for an aggregate if an earlier event remains unpublished or failed.
- **At-Least-Once Delivery**: Downstream consumers deduplicate using `event_id`.

---

### 3.3.3 CloudEvents Schema Standard ([`app/kafka/events/base.py`](file:///home/nazmul/Downloads/fastapi-repo-pattern/app/kafka/events/base.py))

All events adhere to a structured, versioned CloudEvents-compliant envelope:
```json
{
  "event_id": "f81d4fae-7dec-11d0-a765-00a0c91e6bf6",
  "aggregate_id": "c9bf9e57-1685-4c89-bafb-ff5af830be8a",
  "event_type": "user.created.v1",
  "version": 1,
  "timestamp": "2026-11-01T14:30:00.000Z",
  "correlation_id": "req-987654",
  "payload": {
    "user_id": "c9bf9e57-1685-4c89-bafb-ff5af830be8a"
  }
}
```
*Note: Payload contains entity IDs only. PII is excluded to ensure compliance with privacy regulations.*

---

### 3.3.4 Producer Architecture ([`app/kafka/producer.py`](file:///home/nazmul/Downloads/fastapi-repo-pattern/app/kafka/producer.py))

- **Driver**: `aiokafka.AIOKafkaProducer`
- **Reliability Settings**:
  - `acks="all"`: Ensures acknowledgment by the full broker quorum (leader and in-sync replicas).
  - `enable_idempotence=True`: Eliminates duplicate messages from producer retries.
  - `compression_type="gzip"`: Minimizes network bandwidth overhead.

---

### 3.3.5 Consumer Architecture & Strict Manual Offset Commit ([`app/kafka/consumer.py`](file:///home/nazmul/Downloads/fastapi-repo-pattern/app/kafka/consumer.py))

- **The Golden Rule**: **Database transaction commits first, Kafka offset commits second.**
- **Automatic Offsets Disabled**: `enable_auto_commit=False`.
- **Failure Recovery**: If the database transaction or handler fails:
  1. The consumer seeks back to the failed record offset (`consumer.seek(partition, failed_offset)`).
  2. The offset is NOT advanced, ensuring no event is skipped.

---

### 3.3.6 Consumer Retry Policy & Dead Letter Queue ([`app/kafka/dlq.py`](file:///home/nazmul/Downloads/fastapi-repo-pattern/app/kafka/dlq.py))

- **Transient Errors** (Deadlocks, DB connection dropouts): Retried up to 3 times with exponential backoff and random jitter.
- **Permanent Errors** (Malformed JSON, non-existent entity, validation failure):
  - Retrying is futile and would block partition processing.
  - Routed immediately to the Dead Letter Queue topic (`{topic}.DLQ`).
  - DLQ records include diagnostic headers (`x-error-message`, `x-original-topic`, `x-failed-timestamp`, `x-retry-count`).

---

### 3.3.7 Inventory Reservation Saga & TTL Sweeper

Manages high-throughput inventory allocation without race conditions:
1. Consumes `inventory.reserved.v1` events on `KAFKA_INVENTORY_TOPIC`.
2. **PostgreSQL Conditional Hold**:
   - `products.stock` remains physical on-hand stock.
   - `products.reserved_quantity` tracks holds.
   - Available to Promise: `available = stock - reserved_quantity`.
3. If stock is sufficient: Records reservation with expiration timestamp (`ttl_minutes`).
4. If stock is insufficient: Emits `inventory.reservation_failed.v1` event via outbox.
5. **TTL Sweeper Daemon**: Periodically scans expired reservations, releases `reserved_quantity`, and publishes `inventory.reservation_expired.v1`.

---

### 3.3.8 Standalone Background Worker Daemon ([`app/workers/main.py`](file:///home/nazmul/Downloads/fastapi-repo-pattern/app/workers/main.py))

Decouples long-running Kafka event consumption and outbox dispatch from web worker processes:
```bash
python -m app.workers.main
```
- Manages supervisor lifecycle, graceful shutdown on `SIGINT` / `SIGTERM`, and asynchronous consumer loops outside Gunicorn.

---

# Part 4: Database Auto-Initialization, Safe Migrations & Idempotent Seeders

Production systems require safe schema bootstrap, automatic migration locks, and deterministic seeders that never drop tables or overwrite live data.

```
┌─────────────────────────────────────────────────────────────┐
│                    DATABASE MANAGEMENT                      │
├─────────────────────────────────────────────────────────────┤
│ CLI: python -m app.database {status,check,create-tables,...}│
│ CLI: python -m app.database.seeders {list,run}              │
└─────────────────────────────────────────────────────────────┘
```

---

## 4.1 Safe Table Inspection & Auto-Creation Mechanics

- **Opt-in Startup Toggle**: Controlled by environment variable `AUTO_CREATE_TABLES=true`.
- **Table Inspection Engine** ([`app/database/initialization.py`](file:///home/nazmul/Downloads/fastapi-repo-pattern/app/database/initialization.py)):
  - Inspects existing tables via `sa.inspect(connection).get_table_names()`.
  - Calculates difference against SQLAlchemy metadata:
    ```python
    missing = [t for t in Base.metadata.tables.values() if t.name not in existing]
    ```
  - Creates **only missing tables** via `Base.metadata.create_all(tables=missing)`.
  - **Safety Constraint**: If `alembic_version` exists in the database, `create_missing_tables()` refuses to run to protect migration history from schema drift.

---

## 4.2 Alembic Migration Execution & Advisory Locks

- **Opt-in Startup Toggle**: `AUTO_MIGRATE=true`.
- **Distributed Migration Lock** ([`alembic/env.py`](file:///home/nazmul/Downloads/fastapi-repo-pattern/alembic/env.py)):
  - Multiple Gunicorn workers booting simultaneously must not run concurrent schema migrations.
  - Acquired via PostgreSQL session advisory lock:
    ```sql
    SELECT pg_advisory_lock(738291046);
    ```
  - Executes `alembic.command.upgrade(config, "head")`.
  - Releases lock in `finally` block via `SELECT pg_advisory_unlock(738291046);`.

---

## 4.3 Idempotent Seeder Engine & Topological Dependency Resolution

- **Base Seeder Interface** ([`app/database/seeders/base.py`](file:///home/nazmul/Downloads/fastapi-repo-pattern/app/database/seeders/base.py)):
  - Declares seeder `name` and prerequisite `dependencies`.
- **Topological Sorting**:
  - [`app/database/seeders/runner.py`](file:///home/nazmul/Downloads/fastapi-repo-pattern/app/database/seeders/runner.py) builds a directed dependency graph.
  - Sorts seeders so parents are seeded before children (e.g. `category_seeder` runs before `user_seeder`).
  - Validates against circular dependencies, raising `RuntimeError` if a cycle is detected.
- **Idempotency Guarantee**:
  - Seeders verify existing records before inserting (e.g. checking user email blind index).
  - Never overwrites or mutates existing records.

---

## 4.4 Unified Database CLI Reference

Inspect and manage the database directly from the terminal:

### 1. Database Health & Table Inspection
```bash
# Check connectivity, missing tables, and current Alembic migration version
python -m app.database status

# Fast health check (returns exit code 0 if healthy, 1 if missing tables)
python -m app.database check
```

### 2. Table Creation & Migrations
```bash
# Create missing tables (development bootstrap)
python -m app.database create-tables

# Apply pending Alembic migrations
python -m app.database migrate

# Generate new migration revision
python -m app.database revision "add_orders_table"

# View migration history
python -m app.database history

# Roll back last migration
python -m app.database rollback -1
```

### 3. Database Seeders
```bash
# List all registered seeders in topological execution order
python -m app.database.seeders list

# Execute all registered seeders
python -m app.database.seeders run

# Execute a specific seeder
python -m app.database.seeders run user_seeder
```

---

# Part 5: Container-Wise & Environment Configuration Guide

Comprehensive reference for multi-container deployment, host networking, and environment variables.

---

## 5.1 Host vs. Docker Networking & Port Matrix

| Service | Container Name | Host Access Port | Container Network Port | Internal Network Address |
| :--- | :--- | :--- | :--- | :--- |
| **PostgreSQL 16** | `demo_postgres` | *(Internal)* | `5432` | `postgres:5432` |
| **PgBouncer 1.21+** | `demo_pgbouncer` | `6432` | `5432` | `pgbouncer:5432` |
| **Redis Node 1** | `demo_redis_1` | `7000` | `7000` | `redis-1:7000` |
| **Redis Node 2** | `demo_redis_2` | `7001` | `7001` | `redis-2:7001` |
| **Redis Node 3** | `demo_redis_3` | `7002` | `7002` | `redis-3:7002` |
| **Elasticsearch 8** | `demo_elasticsearch` | `9200` | `9200` | `elasticsearch:9200` |
| **Apache Kafka** | `demo_kafka` | *(Internal)* | `9092` | `kafka:9092` |
| **FastAPI App** | `demo_app` | `8000` | `8000` | `app:8000` |
| **Nginx Proxy** | `demo_nginx` | `80` | `80` | `nginx:80` |

---

## 5.2 Environment Variable Dictionary (`.env`)

```dotenv
# ----------------------------------------------------------------------------
# Application Settings
# ----------------------------------------------------------------------------
APP_NAME=HighPerfStatelessService
DEBUG=false
LOG_LEVEL=INFO
ENVIRONMENT=development

# ----------------------------------------------------------------------------
# Database Auto-Initialization & Bootstrap Toggles
# ----------------------------------------------------------------------------
AUTO_CREATE_TABLES=false       # Create missing tables on boot (dev/test only)
AUTO_MIGRATE=false             # Apply Alembic migrations on boot
AUTO_SEED=false                # Run registered seeders on boot

# ----------------------------------------------------------------------------
# PostgreSQL / PgBouncer Credentials & Connection
# ----------------------------------------------------------------------------
DB_USER=postgres
DB_PASSWORD=postgrespassword
DB_HOST=127.0.0.1
DB_PORT=6432
DB_NAME=app_db
DATABASE_URL=postgresql+asyncpg://postgres:postgrespassword@127.0.0.1:6432/app_db
MIGRATION_DATABASE_URL=
PGBOUNCER_ENABLED=true
DB_POOL_SIZE=5
DB_MAX_OVERFLOW=5
DB_POOL_TIMEOUT=30
DB_POOL_RECYCLE=1800

# ----------------------------------------------------------------------------
# Redis 7 Cluster & Standalone Settings
# ----------------------------------------------------------------------------
REDIS_CLUSTER_NODES=127.0.0.1:7000,127.0.0.1:7001,127.0.0.1:7002
REDIS_PASSWORD=redispassword
REDIS_SOCKET_TIMEOUT=2.0
REDIS_CONNECT_TIMEOUT=3.0
REDIS_MAX_CONNECTIONS=50
REDIS_SSL=false

# ----------------------------------------------------------------------------
# Elasticsearch 8 Settings
# ----------------------------------------------------------------------------
ELASTICSEARCH_ENABLED=true
ELASTICSEARCH_URL=http://127.0.0.1:9200
ELASTICSEARCH_INDEX_PREFIX=demo
ELASTICSEARCH_USERNAME=elastic
ELASTICSEARCH_PASSWORD=elasticpassword
ELASTICSEARCH_API_KEY=
ELASTICSEARCH_VERIFY_CERTS=false
ELASTICSEARCH_TIMEOUT=5.0
ELASTICSEARCH_MAX_RETRIES=3
ELASTICSEARCH_LOG_INDEX=app-logs
ELASTICSEARCH_LOG_RETENTION_DAYS=30
ELASTICSEARCH_LOG_BUFFER_SIZE=1000
ELASTICSEARCH_LOG_FLUSH_INTERVAL=2.0
ELASTICSEARCH_LOG_BATCH_SIZE=100

# ----------------------------------------------------------------------------
# Apache Kafka Settings
# ----------------------------------------------------------------------------
KAFKA_ENABLED=true
KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:9092
KAFKA_EVENTS_TOPIC=domain.events
KAFKA_CONSUMER_GROUP=highperf-fastapi
KAFKA_INVENTORY_TOPIC=inventory.events
KAFKA_INVENTORY_CONSUMER_GROUP=highperf-inventory
KAFKA_INVENTORY_DLQ_TOPIC=inventory.events.dlq
OUTBOX_POLL_INTERVAL=1.0
OUTBOX_BATCH_SIZE=100
INVENTORY_RESERVATION_SWEEP_INTERVAL=30.0

# ----------------------------------------------------------------------------
# Cryptography (Self-Managed AES-256-GCM & HMAC Blind Index)
# ----------------------------------------------------------------------------
ENCRYPTION_KEYS={"1": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"}
ACTIVE_KEY_VERSION=1
BLIND_INDEX_KEY=fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210

# ----------------------------------------------------------------------------
# Reverse Proxy Trust Configuration
# ----------------------------------------------------------------------------
TRUSTED_PROXIES=127.0.0.1,10.0.0.0/8,172.16.0.0/12,192.168.0.0/16
```

---

## 5.3 Dynamic Service URL Assembly in Pydantic Settings

In [`app/core/config.py`](file:///home/nazmul/Downloads/fastapi-repo-pattern/app/core/config.py), the `Settings` class utilizes a Pydantic `@model_validator(mode="after")` to automatically assemble and synchronize connection URLs:
- If individual variables (`DB_USER`, `DB_PASSWORD`, `DB_HOST`, `DB_PORT`, `DB_NAME`) are supplied, `DATABASE_URL` is dynamically updated.
- If individual Redis parameters (`REDIS_HOST`, `REDIS_PORT`, `REDIS_PASSWORD`) are supplied and `REDIS_URL` is empty, `REDIS_URL` is automatically assembled.
- If `ELASTICSEARCH_HOST` and `ELASTICSEARCH_PORT` are supplied, `ELASTICSEARCH_URL` is assembled accordingly.
- This ensures full flexibility between local single-variable shell overrides, containerized deployments, and standard `.env` configuration files.
