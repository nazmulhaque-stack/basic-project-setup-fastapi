# Antigravity Workspace Guidelines: Senior Backend & System Architecture

Welcome to the **FastAPI High-Performance Backend Architecture** project.
This repository implements an enterprise-grade, clean architecture backend designed to serve **100,000 (1 lakh) concurrent requests** with asynchronous PostgreSQL/MySQL, PgBouncer transaction pooling, Redis 7 Cluster caching, Elasticsearch 8 CQRS search, AES-256-GCM AEAD cryptography, and Kubernetes deployment.

---

## 1. Architectural Integrity & Layering
- **API Transport (`app/api/`)**: Thin controllers handling HTTP requests, query parameters, and Pydantic schemas. Controllers only talk to the Service layer via `Annotated[Service, Depends(get_service)]`.
  - Dynamic `CRUDRouter` (`app/api/crud_router.py`) dynamically generates validated list query models and routes full-text searches to Elasticsearch.
  - Endpoints always return fast binary serialized responses (`ORJSONResponse`).
- **Domain Services (`app/services/`)**: Centralizes business rules, transaction boundaries (`commit()`, `rollback()`), dynamic cache invalidation, and PII cryptography.
  - Domain services (`UserService`, `ProductService`, `CategoryService`) **must inherit from `BaseService`** and implement pluggable lifecycle hooks (`before_create`, `before_update`, `to_read_schema`, `to_cache_dict`).
  - Polymorphic cache handling via `NullCache` eliminates defensive `if self.cache is not None:` branches.
  - Never duplicate generic CRUD queries or transaction commits in domain services.
- **Repository Persistence (`app/repositories/`)**: Encapsulates all SQLAlchemy 2.0 Async queries, dynamic filtering (`eq`, `ne`, `gt`, `gte`, `lt`, `lte`, `in`, `like`, `ilike`), ordering, and Keyset cursor pagination.
  - **Repositories must NEVER call `commit()` or `rollback()`**. They only flush state changes (`await session.flush()`).
  - Shared `AsyncSession` dependency enables atomic multi-repository business transactions.
- **Database Models (`app/models/`)**: Declarative SQLAlchemy models inheriting from `Base` (`app/models/base.py` / `app/db/base.py`).
- **Schemas (`app/schemas/`)**: Pydantic v2 DTOs (`model_config = ConfigDict(from_attributes=True)`).

---

## 2. Database Connection Multiplexing & PgBouncer
- **Transaction Pooling Mode**: PgBouncer multiplexes client sockets across pre-warmed backend connections to PostgreSQL.
- **`statement_cache_size = 0` (Mandatory)**: In `app/core/database.py`, `connect_args={"statement_cache_size": 0, "prepared_statement_cache_size": 0}` is strictly required to prevent `prepared statement "_asyncpg_stmt_*" does not exist` crashes across pooled connections.
- **Connection Sizing Formula for 100k Concurrency**:
  $$\text{Total App Sockets} = \text{Nodes (16)} \times \text{Workers/Node (8)} \times (\text{pool\_size} (5) + \text{max\_overflow} (5)) = 1,280 \le \text{PgBouncer MAX\_CLIENT\_CONN} (2,000)$$
  $$\text{PgBouncer DEFAULT\_POOL\_SIZE} (80) \le \text{PostgreSQL max\_connections} (150) - 30 \text{ Reserved} = 120$$

---

## 3. CQRS Search Engine & Elasticsearch 8
- **Command-Query Responsibility Segregation (CQRS)**: PostgreSQL is the authoritative system of record for all writes.
- **Elasticsearch Integration (`app/core/elasticsearch.py`)**: Asynchronously syncs domain documents for fuzzy search, full-text ranking, and faceted aggregations.
- **Graceful Degradation (`NullElasticsearch`)**: If Elasticsearch is unavailable or disabled via `ELASTICSEARCH_ENABLED=false`, search operations degrade gracefully without breaking primary CRUD workflows.

---

## 4. Distributed Caching & Redis 7 Cluster
- **MurmurHash3 Item Sharding**: Single item keys (`cache:<prefix>:item:<id>`) have NO hash tags, ensuring uniform distribution across Redis Cluster master nodes.
- **$O(1)$ Namespace Versioning for Lists**: List keys include version counters (`cache:<prefix>:list:v{version}:{hash}`). Invalidation executes an atomic `INCR cache:<prefix>:list_ver`, avoiding expensive and cluster-failing `CROSSSLOT` multi-key deletes.
- **Stampede Mutex Locking**: Protect hot entity cache misses using `SET NX PX` distributed locks with 50ms backoff.
- **Negative Caching**: Cache missing IDs for 30s using sentinel `{"__not_found__": true}` to prevent database penetration attacks.
- **Dynamic TTL Jitter**: All cached items receive ±10% randomized TTL jitter to prevent synchronized expiration waves.
- **Per-Call & Runtime Overrides**: Support `cache_enabled=False` on individual service calls for immediate post-mutation consistency checks.

---

## 5. Cryptography & PII Security (AES-256-GCM AEAD)
- **AES-256-GCM Wire Format**: Tokens stored as URL-safe Base64: `[version(1B) || nonce(12B) || ciphertext+tag(N+16B)]`.
- **96-bit Random Nonces**: Generated fresh via `os.urandom(12)` on every write; nonces are never reused.
- **AAD Context Binding**: Always bind ciphertext to entity identity (`aad=f"users.phone:{user_id}".encode("utf-8")`) to neutralize ciphertext transplantation attacks.
- **Zero-Downtime Key Rotation**: Byte 0 indicates the encryption key version. Decryption resolves the corresponding cipher, while new writes use `ACTIVE_KEY_VERSION`.
- **HMAC-SHA256 Blind Indexing**: Deterministic hashing with a dedicated secret key enables exact-match lookups and unique database constraints without decrypting records.
- **Serialization Boundary Masking**: Read schemas and cache dictionaries must always mask PII (`+1******2671`).

---

## 6. 100,000 (1 Lakh) Concurrent Requests System Architecture
- **Layer 4 Ingress**: Anycast BGP / ECMP / HAProxy load balancing distributes raw TCP streams across 13–20 reverse proxy nodes.
- **Reverse Proxy (Nginx)**: `epoll` event loop, `worker_connections 65535`, persistent upstream `keepalive 128` sockets, connection limit `limit_conn 100/IP`, rate limit `limit_req 100r/s burst=50 nodelay`.
- **Application Pool (Gunicorn + Uvicorn)**: 1 worker per core via `multiprocessing.cpu_count()`, C-based `uvloop` (epoll) event loop, worker recycling (`max_requests=10000`, `jitter=1000`), heartbeats in `/dev/shm`.
- **Linux Kernel TCP Tuning (`deploy/sysctl.conf`)**:
  - `net.core.somaxconn = 65535`, `net.ipv4.tcp_max_syn_backlog = 65535`
  - `net.ipv4.tcp_tw_reuse = 1`, `net.ipv4.tcp_fin_timeout = 15`
  - `fs.file-max = 2097152`, `nofile = 1048576`
- **Atomic Lua Rate Limiting (`app/services/redis_rate_limit.py`)**: Real IP extraction verified against `TRUSTED_PROXIES`, cluster slot-pinned keys `rl:{<scope>:<ip>}:<window>`, atomic Lua script execution via `EVALSHA`.

---

## 7. Query Performance & Keyset Pagination
- Large list endpoints must use **Keyset (cursor) pagination** (`order_by(created_at, id)` composite seek) instead of deep offset scans.
- Cursor token format: URL-safe Base64 of `timestamp|uuid`.
- Prevent N+1 queries by using `selectinload` or `joinedload` on relationships.

---

## 8. Development & Testing Commands
- **Run Quality Pipeline**: `bash .agents/agents/developer-agent/skills/run_linter.sh`
- **Security Audit**: `python3 .agents/agents/reviewer-agent/skills/security_audit.py`
- **Database Model Inspection**: `python3 .agents/agents/developer-agent/skills/db_migration_helper.py`
- **Infrastructure Verification**: `python3 .agents/agents/system-architect-agent/skills/verify_infrastructure.py`
- **Run Pytest Suite**: `pytest -v`
