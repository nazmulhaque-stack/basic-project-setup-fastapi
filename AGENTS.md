# Antigravity Workspace Guidelines: Senior Backend & System Architecture

Welcome to the **FastAPI High-Performance Backend Architecture** project.
This repository implements an enterprise-grade, clean architecture backend with asynchronous PostgreSQL/MySQL, Redis Cluster caching, AES-256-GCM cryptography, and Kubernetes deployment.

---

## 1. Architectural Integrity & Layering
- **API Transport (`app/api/`)**: Thin controllers handling HTTP requests, query parameters, and Pydantic schemas. Controllers only talk to the Service layer via `Annotated[Service, Depends(get_service)]`.
- **Domain Services (`app/services/`)**: Centralizes business rules, transaction boundaries (`commit()`, `rollback()`), cache invalidation, and PII cryptography.
  - Domain services (`UserService`, `ProductService`) **must inherit from `BaseService`** and implement pluggable lifecycle hooks (`before_create`, `before_update`, `to_read_schema`, `to_cache_dict`).
  - Never duplicate generic CRUD queries or transaction commits in domain services.
- **Repository Persistence (`app/repositories/`)**: Encapsulates all SQLAlchemy 2.0 Async queries, dynamic filtering, ordering, and Keyset cursor pagination.
  - **Repositories must NEVER call `commit()` or `rollback()`**. They only flush state changes (`await session.flush()`).
- **Database Models (`app/models/`)**: Declarative SQLAlchemy models inheriting from `Base`.
- **Schemas (`app/schemas/`)**: Pydantic v2 DTOs (`model_config = ConfigDict(from_attributes=True)`).

---

## 2. Distributed Caching & Redis Cluster
- **MurmurHash3 Item Sharding**: Single item keys (`cache:<prefix>:item:<id>`) have NO hash tags, ensuring uniform distribution across Redis Cluster master nodes.
- **$O(1)$ Namespace Versioning for Lists**: List keys include version counters (`cache:<prefix>:list:v{version}:{hash}`). Invalidation executes an atomic `INCR cache:<prefix>:list_ver`, avoiding expensive and cluster-failing `CROSSSLOT` multi-key deletes.
- **Stampede Mutex Locking**: Protect hot entity cache misses using `SET NX PX` distributed locks.
- **Negative Caching**: Cache missing IDs for 30s to prevent database penetration attacks.

---

## 3. Cryptography & PII Security
- **AES-256-GCM**: Sensitive fields (e.g., phone numbers) must be encrypted with 96-bit nonces.
- **AAD Context Binding**: Always bind ciphertext to entity identity (`aad=f"users.phone:{user_id}".encode("utf-8")`) to prevent ciphertext transplantation attacks.
- **HMAC-SHA256 Blind Indexing**: Deterministic hashing enables exact-match database queries without decrypting records.
- **Masking**: Public read schemas must always mask PII (`+1******2671`).

---

## 4. Query Performance & Keyset Pagination
- Large list endpoints must use **Keyset (cursor) pagination** (`order_by(created_at, id)` composite seek) instead of deep offset scans.
- Prevent N+1 queries by using `selectinload` or `joinedload` on relationships.

---

## 5. Development & Testing Commands
- **Run Quality Pipeline**: `bash .agents/agents/developer-agent/skills/run_linter.sh`
- **Security Audit**: `python3 .agents/agents/reviewer-agent/skills/security_audit.py`
- **Database Model Inspection**: `python3 .agents/agents/developer-agent/skills/db_migration_helper.py`
- **Infrastructure Verification**: `python3 .agents/agents/system-architect-agent/skills/verify_infrastructure.py`
- **Run Pytest Suite**: `pytest -v`
