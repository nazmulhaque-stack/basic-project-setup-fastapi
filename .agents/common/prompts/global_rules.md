# Global Architecture, Coding & Security Constraints

## 1. System Overview & Technology Stack
- **Language**: Python 3.12+ (Strict type annotations, PEP 585/PEP 604 union types `A | B`).
- **Framework**: FastAPI (Asynchronous request handling, Dependency Injection via `Annotated[..., Depends(...)]`).
- **Data Validation & Serialization**: Pydantic v2 (`BaseModel`, `ConfigDict(from_attributes=True)`).
- **Data Access**: Raw `asyncpg` (no ORM). A `UnitOfWork` wrapper (`app/db/pool.py`) gives one acquired connection + transaction a `commit()`/`rollback()` interface; each repository hand-writes its own SQL per entity — no generic filter/operator engine.
- **Caching & Rate Limiting**: Redis 7+ Cluster (Namespace versioning, stampede mutex locks, atomic Lua scripts).
- **Security & PII**: AES-256-GCM authenticated encryption with AAD, HMAC-SHA256 blind indexing, phone number masking.
- **Infrastructure & Deployment**: Nginx Reverse Proxy, Docker Multi-Stage Builds, Kubernetes (K8s), Gunicorn + Uvicorn Workers.

---

## 2. Architectural Boundaries & Decoupling
1. **Clean Separation of Concerns**:
   - **Transport Layer (`app/api/`)**: Thin controllers handling HTTP requests, response schemas, and status codes. No direct database queries or raw business rules.
   - **Service Layer (`app/services/`)**: Orchestrates business rules, domain lifecycle hooks (`before_create`, `before_update`, `before_delete`, `after_create`, `after_update`, `after_delete`, `to_read_schema`), transaction boundaries (`commit()`, `rollback()`), and cache eviction.
   - **Repository Layer (`app/repositories/`)**: Hand-written asyncpg SQL per entity — filtering, ordering, and keyset cursor seek logic. **Repositories must NEVER commit transactions**—they only `fetch`/`fetchrow`/`fetchval`/`execute` on the `UnitOfWork` (no separate flush step: asyncpg statements apply immediately inside the still-open transaction).
   - **Database Models (`app/models/`)**: Plain `@dataclass` models inheriting from `Base` (no ORM).
   - **Schemas (`app/schemas/`)**: Pydantic models for request ingestion, payload validation, and public serialization.

2. **Template Method Pattern**:
   - Generic CRUD workflows are centralized in `BaseService`.
   - Domain services (`UserService`, `ProductService`) implement pluggable domain hooks rather than re-writing duplicate CRUD queries and transaction handling.

---

## 3. Database & Transactional Integrity
1. **Transaction Ownership**:
   - The Service layer controls database transaction boundaries.
   - Write operations must follow this exact sequence:
     $$\text{Validation/Hooks} \longrightarrow \text{Repository Write} \longrightarrow \text{Flush/Commit} \longrightarrow \text{Cache Invalidation} \longrightarrow \text{Read Schema Transformation}$$
   - On `asyncpg.exceptions.IntegrityConstraintViolationError`, the service must execute `await self.db.rollback()` and raise a structured `ConflictError`.

2. **Exception Mapping**:
   - Services must **never** raise FastAPI's `HTTPException` directly.
   - Services raise domain exceptions (`NotFoundError`, `ConflictError`, `ValidationError`).
   - The API layer maps domain exceptions to appropriate HTTP status codes (404, 409, 422).

3. **Query Optimization**:
   - Zero tolerance for N+1 queries. There is no `selectinload`/`joinedload` — a relationship that genuinely needs eager-loading gets a dedicated repository method doing a second hand-written query (e.g. `ProductRepository.get_with_category`), never a per-row query loop.
   - Paginated endpoints with large datasets must use **Keyset (cursor) pagination** (`ORDER BY created_at, id` with a composite seek `WHERE` clause — see `app/repositories/sql_helpers.py:keyset_seek_clause`) to avoid $O(N)$ database offset degradation.

---

## 4. Distributed Caching & Redis Cluster Rules
1. **Cluster Slot Pinning vs MurmurHash3 Sharding**:
   - **Single Item Keys**: `cache:<prefix>:item:<id>` (NO hash tag). Distributed evenly across all master nodes via MurmurHash3.
   - **Rate Limit Keys**: `rl:{<scope>:<identity>}:<window>` (Pinned via hash tag `{...}` to a single hash slot for atomic multi-key Lua execution).
2. **$O(1)$ Namespace Versioning for Lists**:
   - Never use Set-Tracking or multi-key `DEL` across cluster shards (causes `CROSSSLOT` failures).
   - Invalidate list caches by atomically incrementing the namespace version: `INCR cache:<prefix>:list_ver`.
3. **Cache Stampede & Negative Caching**:
   - Use short-lived distributed mutex locks (`SET NX PX`) on hot cache misses.
   - Cache non-existent lookups for 30 seconds (`NEGATIVE_CACHE_SENTINEL = {"__not_found__": True}`) to prevent cache penetration DDoS attacks on PostgreSQL/MySQL.

---

## 5. Cryptography & PII Protection
1. **AES-256-GCM Encryption**:
   - Encrypt sensitive PII (phone numbers, personal identification) with random 96-bit nonces.
   - Cryptographically bind ciphertext to entity identity using Additional Authenticated Data (`aad=f"{table}.{field}:{entity_id}"`).
2. **HMAC-SHA256 Blind Indexing**:
   - Use a separate, HMAC-SHA256 blind indexing key for exact-match lookups on encrypted columns without decrypting database records.
3. **Data Masking**:
   - Plaintext PII must never be stored in the database or serialized into public API responses. Phone numbers must be masked (e.g., `+1******2671`).

---

## 6. Infrastructure, Docker & Kubernetes
1. **Container Security**:
   - Dockerfiles must use multi-stage builds and run as a non-privileged user (`USER appuser`).
   - Minimal attack surface: base images should use `python:3.12-slim`.
2. **Nginx Reverse Proxy & Rate Limiting**:
   - Nginx handles TLS termination, HTTP/2, buffer overflow protection, and initial connection throttling.
   - Application rate limiting uses trusted proxy CIDR validation (`TRUSTED_PROXIES`) before trusting `X-Forwarded-For`.
3. **Kubernetes Standards**:
   - Health probes must bind to `/health/livez` (liveness) and `/health/readyz` (readiness).
   - Horizontal Pod Autoscaler (HPA) configured based on CPU/Memory and custom request latency metrics.
   - Graceful shutdown handles `SIGTERM` with minimum 30s termination grace period to drain active HTTP requests.
