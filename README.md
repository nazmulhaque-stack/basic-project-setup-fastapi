# 🚀 High-Performance Stateless FastAPI Backend Architecture

Production-grade, horizontally scalable, stateless FastAPI backend designed to serve **100,000 (1 lakh) concurrent requests** across self-managed Linux infrastructure without cloud vendor lock-in. Built with **Generic Repository Pattern + Service Layer + SQLAlchemy 2.0 Async + PgBouncer + Redis 7 Cluster + Elasticsearch 8 + Apache Kafka + AES-256-GCM AEAD Security**.

This single document is the complete reference: architecture and sizing math, the full request execution lifecycle, every infrastructure deep dive (crypto, caching, search, streaming, connection pooling), and the step-by-step developer guide for extending the codebase.

---

## ✅ Current Codebase Review Snapshot

This repository is a real FastAPI backend project rather than a toy scaffold. The implementation is organized around a layered architecture:

- [app/main.py](app/main.py): application bootstrap, lifespan lifecycle, structured logging, Redis startup, Elasticsearch startup, and rate-limiter initialization.
- [app/api/v1](app/api/v1): versioned domain API routes for users, products, categories, employees, and logs, assembled through [app/api/v1/router.py](app/api/v1/router.py). Health probes remain at `/healthz` and `/readyz` for proxy and deployment checks.
- [app/api/crud_router/](app/api/crud_router/): generic CRUD endpoint generator for list/create/get/update/delete flows with filtering, sorting, and search integration — split into `query_model.py` (dynamic filter/sort model builder), `routes.py` (CRUD route factories), `search.py` (Elasticsearch search wiring), and `router.py` (the `CRUDRouter` class composing them via a `ROUTE_BUILDERS` registry).
- [app/repositories/base.py](app/repositories/base.py): generic async repository layer with filter operators, protection for reserved fields, integrity handling, and pagination logic.
- [app/services/base.py](app/services/base.py): generic service layer with Redis cache-aside logic, rate-limit hooks, transactional outbox hooks, and template-method flow for create/read/update/delete operations.
- [app/models](app/models): declarative SQLAlchemy models for users, products, categories, employees, and related entities.
- [app/core/config.py](app/core/config.py) and [app/db/session.py](app/db/session.py): environment-driven settings, async DB engine creation, and connection tuning for PostgreSQL/PgBouncer.
- [app/core/kafka.py](app/core/kafka.py) and [app/services/outbox.py](app/services/outbox.py): asynchronous Kafka publishing/consuming and transactional outbox dispatch for service-to-service domain events.
- [app/core/security.py](app/core/security.py): AES-256-GCM encryption, key rotation helpers, masking utilities, and blind-index support for PII fields.

### Reality check from the current codebase

- The project is designed for production-grade asynchronous operations and includes operational concerns such as Redis caching, Elasticsearch integration, request tracing, and structured log shipping.
- The generic repository and service abstraction is implemented coherently, and the domain services follow the expected lifecycle pattern (`before_create`, `before_update`, `before_delete`, `after_create`, `after_update`, `after_delete`, `to_read_schema`, `to_cache_dict`).
- The code intentionally reflects a high-scale architecture narrative (PgBouncer, Redis Cluster, keyset pagination, rate limiting, TLS/PII handling), and the majority of the implementation is consistent with that design.
- The codebase is not just documentation-only; it contains real endpoints, models, services, repositories, schemas, and tests.

### Validation status

The current test result is maintained by running `pytest -q`; the earlier 58-test snapshot predates Kafka inventory consumption and database initialization.

### Event-driven architecture at a glance

When `KAFKA_ENABLED=true`, transactional outbox events can be staged to `outbox_events` within the same PostgreSQL transaction as the domain mutation. The background dispatcher publishes committed rows to `KAFKA_EVENTS_TOPIC`, keyed by aggregate ID; it will not publish a later event for an aggregate while an earlier event remains unpublished. Delivery is at least once: downstream consumers should deduplicate using `event_id` and make handlers idempotent. Event payloads contain the entity ID only; do not add PII without an explicit event-contract and privacy review. The full mechanics, event-naming convention, and a step-by-step guide for building new events live in [§7 Deep Dive: Apache Kafka, Transactional Outbox & Event-Driven Architecture](#-deep-dive-apache-kafka-transactional-outbox--event-driven-architecture) and [§9.11 Building Service-to-Service Events with Kafka](#911-building-service-to-service-events-with-kafka).

With `KAFKA_ENABLED=true`, the API also consumes `KAFKA_INVENTORY_TOPIC` (`inventory.events` by default) in its dedicated `KAFKA_INVENTORY_CONSUMER_GROUP`. It handles `inventory.reserved.v1` events containing `event_id`, `event_type`, `timestamp`, and a payload with `reservation_id`, `order_id`, `product_id`, `quantity`, and `ttl_minutes`; `location_id` is optional metadata in this global-inventory setup. `product_id` may be a product UUID or SKU. Reservations run in a PostgreSQL transaction and are idempotent by event and reservation ID — see [§7.7 Inventory Reservation Saga & TTL Sweeper](#77-inventory-reservation-saga--ttl-sweeper) for the full mechanics.

The Compose Kafka broker is for local development. For production, provision durable replicated topics, broker authentication/TLS, monitoring, and a multi-broker cluster.

---

## 📑 Table of Contents

1. [Architecture & System Topology](#-architecture--system-topology)
    - [High-Contrast Visual Mermaid Architecture](#high-contrast-visual-mermaid-architecture)
    - [Unicode / ASCII Architecture Diagram](#unicode--ascii-architecture-diagram-100-theme-visible)
    - [System Sizing, Math & Connection Budget for 100k Concurrency](#system-sizing-math--connection-budget-for-100k-concurrency)
2. [Project Directory Layout](#-project-directory-layout)
3. [1 Lakh (100k) Concurrent Requests: System Execution Flow](#-1-lakh-100k-concurrent-requests-system-execution-flow)
    - [Visual Sequence Diagram: End-to-End Request Path](#visual-sequence-diagram-end-to-end-request-path)
    - [Phase 1: Ingress, Network Sockets & Linux Kernel TCP Tuning](#phase-1-ingress-network-sockets--linux-kernel-tcp-tuning)
    - [Phase 2: Multi-Host / Multi-Container Distribution (L4 & Nginx)](#phase-2-multi-host--multi-container-distribution-l4--nginx)
    - [Phase 3: Multi-Process Asynchronous Worker Pool (Gunicorn + Uvicorn)](#phase-3-multi-process-asynchronous-worker-pool-gunicorn--uvicorn)
    - [Phase 4: Write Path Call Stack (POST /users)](#phase-4-step-by-step-function-execution-call-stack-write-path-post-users)
    - [Phase 5: Read Path Call Stack (GET /users/{id})](#phase-5-step-by-step-function-execution-call-stack-read-path-get-usersid)
    - [Phase 6: Keyset Cursor Pagination Path](#phase-6-high-concurrency-keyset-cursor-pagination-path)
    - [Phase 7: Elasticsearch Search & Async Logging Lifecycle](#phase-7-elasticsearch-search--async-logging-lifecycle)
    - [Phase 8: Relational & Nested Query Execution Lifecycle](#phase-8-relational--nested-query-execution-lifecycle)
4. [Deep Dive: Encryption, AAD, Key Rotation & Blind Indexing](#-deep-dive-encryption-aad-key-rotation--blind-indexing)
5. [Deep Dive: Distributed Caching, Stampede Locks & Namespace Invalidation](#-deep-dive-distributed-caching-stampede-locks--namespace-invalidation)
6. [Deep Dive: Elasticsearch 8 Search & Structured Logging](#-deep-dive-elasticsearch-8-search--structured-logging)
7. [Deep Dive: Apache Kafka, Transactional Outbox & Event-Driven Architecture](#-deep-dive-apache-kafka-transactional-outbox--event-driven-architecture)
8. [Deep Dive: Database Connection Multiplexing (PgBouncer)](#-deep-dive-database-connection-multiplexing-pgbouncer)
9. [Developer Implementation Workflow (Step-by-Step Guide)](#-developer-implementation-workflow-step-by-step-guide)
    - [9.1 File Modification Checklist](#91-file-modification-checklist)
    - [9.2 How Developers Add a New Entity (Order Example)](#92-how-developers-add-a-new-entity-order-example)
    - [9.3 Using BaseRepository Primitives](#93-using-baserepository-primitives)
    - [9.4 Unit of Work & Multi-Repository Transactions](#94-unit-of-work--multi-repository-transactions)
    - [9.5 The NullCache / Null Object Pattern](#95-the-nullcache--null-object-pattern)
    - [9.6 Overriding Service Lifecycle Hooks](#96-overriding-service-lifecycle-hooks)
    - [9.7 Controlling Transactions & Unit of Work Boundary](#97-controlling-transactions--unit-of-work-boundary)
    - [9.8 Controlling Cache Dynamically (Per-Operation & Per-Call)](#98-controlling-cache-dynamically-per-operation--per-call)
    - [9.9 Writing Automated Tests for a New Entity](#99-writing-automated-tests-for-a-new-entity)
    - [9.10 Nested Tables & Entity Relationships Guide (Category in Product)](#910-nested-tables--entity-relationships-guide-category-in-product)
    - [9.11 Building Service-to-Service Events with Kafka](#911-building-service-to-service-events-with-kafka)
10. [API Reference with Full Examples](#-api-reference-with-full-examples)
11. [Testing Strategy](#-testing-strategy)
12. [Database Initialization, Migrations & Seeders](#-database-initialization-migrations--seeders)
13. [Environment & Container Networking Reference](#-environment--container-networking-reference)
14. [Local Development & Production Deployment Guide](#-local-development--production-deployment-guide)
15. [Why This Stack Is Fast — Technology Choices & Best-Practice Rationale](#-why-this-stack-is-fast--technology-choices--best-practice-rationale)
    - [Audit Fixes Applied to Match the Claims Below](#audit-fixes-applied-to-match-the-claims-below)

---

<a id="-architecture--system-topology"></a>
## 🏛️ Architecture & System Topology

<a id="high-contrast-visual-mermaid-architecture"></a>
### High-Contrast Visual Mermaid Architecture

> **Note on Visibility**: Styled with explicit, high-contrast container fills, crisp outlines, and white typography so all nodes, subgraphs, and data flows render clearly in dark mode, light mode, and high-DPI displays.

<p align="center">
  <img src="assets/architecture.png" alt="High-Contrast Visual Mermaid Architecture" width="100%" />
</p>

---

<a id="unicode--ascii-architecture-diagram-100-theme-visible"></a>
### Unicode / ASCII Architecture Diagram (100% Theme Visible)

For instant readability across raw terminals, plain-text viewers, and all IDE color schemes:

```
┌─────────────────────────────────────────────────────────────────────────────────────────────┐
│                            100,000 CONCURRENT CLIENT CONNECTIONS                            │
│                  (Web Browsers, Mobile Apps, Microservices, Partner APIs)                   │
└─────────────────────────────────────────────────────────────────────────────────────────────┘
                                               │ (100,000 Incoming TCP Sockets)
                                               ▼
┌─────────────────────────────────────────────────────────────────────────────────────────────┐
│                  LAYER 4 INFRASTRUCTURE LOAD BALANCER (Anycast BGP / ECMP)                  │
│  • Distributes raw TCP packets at line-rate across multiple reverse proxies                 │
└─────────────────────────────────────────────────────────────────────────────────────────────┘
                        │                                                             │
                        ▼                                                             ▼
┌───────────────────────────────────────────┐   ┌───────────────────────────────────────────┐
│        NGINX REVERSE PROXY NODE 1         │   │        NGINX REVERSE PROXY NODE N         │
│  • worker_connections: 65535 (epoll)      │   │  • worker_connections: 65535 (epoll)      │
│  • limit_conn: 100 per client IP          │   │  • limit_conn: 100 per client IP          │
│  • limit_req: 100r/s burst=50 nodelay     │   │  • limit_req: 100r/s burst=50 nodelay     │
│  • Upstream keepalive pool: 128 sockets   │   │  • Upstream keepalive pool: 128 sockets   │
└───────────────────────────────────────────┘   └───────────────────────────────────────────┘
                      │ (Persistent Keepalive TCP)
                      ▼
┌─────────────────────────────────────────────────────────────────────────────────────────────┐
│                   APPLICATION TIER: MULTI-HOST / MULTI-CONTAINER CLUSTER                    │
│                        (16 Servers x 8 Cores = 128 Worker Processes)                        │
│                                                                                             │
│ ┌─────────────────────────────────────┐   ┌─────────────────────────────────────┐           │
│ │  Gunicorn Master Supervisor Node 1  │   │  Gunicorn Master Supervisor Node N  │           │
│ │  ├─ Uvicorn Worker 1 (uvloop epoll) │   │  ├─ Uvicorn Worker 1 (uvloop epoll) │           │
│ │  ├─ Uvicorn Worker 2 (uvloop epoll) │   │  ├─ Uvicorn Worker 2 (uvloop epoll) │           │
│ │  └─ ... (8 workers per node)        │   │  └─ ... (8 workers per node)        │           │
│ │                                     │   │                                     │           │
│ │  [ FastAPI Stateless Application ]  │   │  [ FastAPI Stateless Application ]  │           │
│ │  • Request ID & Tracing Middleware  │   │  • Request ID & Tracing Middleware  │           │
│ │  • Redis Lua RateLimiter (EVALSHA)  │   │  • Redis Lua RateLimiter (EVALSHA)  │           │
│ │  • Pydantic v2 Schema Validators    │   │  • Pydantic v2 Schema Validators    │           │
│ │  • Service Layer (Hooks & Crypto)   │   │  • Service Layer (Hooks & Crypto)   │           │
│ │  • BaseRepository (session.flush)   │   │  • BaseRepository (session.flush)   │           │
│ │  • Unit of Work (get_db commit)     │   │  • Unit of Work (get_db commit)     │           │
│ └─────────────────────────────────────┘   └─────────────────────────────────────┘           │
└─────────────────────────────────────────────────────────────────────────────────────────────┘
                      │                                               │
           ┌──────────┴────────────────────┐          ┌───────────────┴────────────────────┐
           │ Dynamic Cache / Rate Limiting  │          │ Relational Data Persistence         │
           ▼                                 ▼         ▼                                     ▼
┌──────────────────────────────────────────────┐   ┌──────────────────────────────────────────┐
│          REDIS 7 CLUSTER (3+ NODES)          │   │     PGBOUNCER 1.21+ TRANSACTION POOL     │
│  • 16,384 Hash Slots with Slot Pinning {key} │   │  • max_client_conn = 2,000 Sockets       │
│  • Lua EVALSHA Atomic Rate Limiting          │   │  • default_pool_size = 80 Backend Conn   │
│  • Dynamic Cache-Aside (get_item / set_item) │   │  • statement_cache_size = 0 (Mandatory)  │
│  • O(1) Namespace Invalidation (INCR)        │   └──────────────────────────────────────────┘
│  • Mutex Stampede Locks (SET NX PX)          │                       │ (80 Multiplexed Conn) 
│  • Negative Cache Sentinels (__not_found__)  │                       ▼                       
└──────────────────────────────────────────────┘   ┌──────────────────────────────────────────┐
                                                   │       POSTGRESQL 16 PRIMARY ENGINE       │
                                                   │  • max_connections = 150 (30 Reserved)   │
                                                   │  • B-Tree Keyset Index: (created_at, id) │
                                                   │  • UNIQUE Index: (phone_blind_index)     │
                                                   │  • Zero Long-Lived Client Connections    │
                                                   └──────────────────────────────────────────┘
```

---

<a id="system-sizing-math--connection-budget-for-100k-concurrency"></a>
### System Sizing, Math & Connection Budget for 100k Concurrency

Serving 100,000 simultaneous connections without resource exhaustion requires a strict connection budget across all tiers:

| Tier                    | Component                          | Sizing for 100k Concurrency                                          | Math & Configuration                                                                                                 | Architectural Rationale                                                                                                                                                  |
| ----------------------- | ---------------------------------- | -------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| **L4 Ingress**          | Anycast / ECMP / HAProxy           | Handles 100,000 simultaneous TCP sockets                             | Line-rate stream forwarding                                                                                          | Terminates zero HTTP traffic; routes IP packets to proxy tier.                                                                                                           |
| **L7 Reverse Proxy**    | Multi-Host Nginx (13–20 instances) | 65,535 connections per node                                          | `worker_connections 65535;`<br/>`keepalive 128;`<br/>`limit_conn 100;`                                               | Non-blocking `epoll` event loop buffers client slow-loris bursts; reuses persistent keepalive TCP connections to FastAPI workers.                                        |
| **Process Manager**     | Gunicorn 23.0.0                    | 8 workers per 8-core host (16 hosts = 128 workers)                   | `workers = multiprocessing.cpu_count()`<br/>`worker_tmp_dir = "/dev/shm"`<br/>`max_requests = 10000; jitter = 1000;` | 1 worker per physical core achieves CPU saturation without context-switch thrashing. Periodic jittered worker recycling prevents memory fragmentation.                   |
| **Async Event Loop**    | Uvicorn + uvloop 0.34.0            | 128 total worker processes across cluster                            | **~800 to 1,200 concurrent sockets per worker process**                                                              | Written in C on top of `libuv`. Switches asynchronously on socket I/O events without spawning OS threads.                                                                |
| **App Framework**       | Stateless FastAPI 0.115.8          | Zero shared in-process state                                         | Default response class: `ORJSONResponse`                                                                             | Rust-based binary JSON encoding; dependency injection manages scoped unit-of-work sessions.                                                                              |
| **Connection Pool**     | PgBouncer 1.21+                    | Multiplexes 1,280 client connections into **80 backend connections** | `POOL_MODE = transaction`<br/>`MAX_CLIENT_CONN = 2000`<br/>`DEFAULT_POOL_SIZE = 80`                                  | PostgreSQL spawns 1 OS process per connection (10MB RAM each). PgBouncer holds idle client sockets and attaches a Postgres connection ONLY while a SQL transaction runs. |
| **Relational Database** | PostgreSQL 16 Primary              | Fixed `max_connections = 150`                                        | `shared_buffers = 512MB`<br/>`work_mem = 4MB`<br/>Composite index: `(created_at, id)`                                | Database operates at peak throughput without memory thrashing because backend connections are strictly capped below 100.                                                 |
| **Distributed Cache**   | Redis 7 Cluster                    | 3+ Master nodes (16,384 hash slots)                                  | Slot pinning `{users:id}`<br/>`max_connections = 500` per worker pool                                                | Absorbs 95%+ of read traffic; executes atomic Lua rate limits in < 1 ms.                                                                                                 |

---

<a id="-project-directory-layout"></a>
## 📁 Project Directory Layout

The codebase enforces strict separation of concerns across HTTP controllers, domain business logic, data persistence, and core infrastructure:

```
app/
├── core/
│   ├── config.py           # Settings (pydantic-settings, reads .env)
│   ├── database.py         # async engine, session factory, get_session dependency
│   ├── security.py         # AES-256-GCM CryptoService, blind indexing, masking
│   ├── redis.py            # Redis Cluster client, distributed lock primitive
│   ├── elasticsearch.py    # AsyncElasticsearchService + NullElasticsearch fallback
│   ├── kafka.py            # Kafka producer/consumer primitives
│   ├── logging.py          # Structured logging, AsyncLogBuffer, ECS formatter
│   └── exceptions.py       # AppError → NotFound(404) / Conflict(409) / BusinessRule(422)
├── models/                 # SQLAlchemy 2 typed models
│   ├── base.py             # Base + IdMixin + TimestampMixin + constraint naming
│   ├── category.py
│   ├── user.py
│   ├── product.py
│   ├── employee.py
│   └── outbox.py           # OutboxEvent (transactional outbox table)
├── schemas/                # Pydantic v2
│   ├── base.py             # BaseSchema, InputSchema, ReadSchema, Page[T], make_partial()
│   ├── category.py
│   ├── user.py
│   └── product.py
├── repositories/           # data access only
│   ├── base.py             # IBaseRepository (ABC) + generic BaseRepository[Model]/SQLAlchemyRepository[Model]
│   ├── category.py
│   ├── user.py             # 1 line: inherits everything
│   └── product.py          # + custom queries (eager load, atomic stock update)
├── services/               # business rules + transaction boundary
│   ├── base.py             # BaseService[Model, Create, Update] template-method CRUD + hooks
│   ├── cache.py            # DynamicRedisCache cache-aside manager (split out of base.py)
│   ├── category.py
│   ├── user.py             # 1 line
│   ├── product.py          # rules via hooks + custom use cases
│   ├── outbox.py           # OutboxDispatcher background daemon
│   ├── inventory_reservations.py
│   └── redis_rate_limit.py # Lua EVALSHA sliding-window rate limiter
├── kafka/
│   ├── producer.py         # AIOKafkaProducer wrapper (acks=all, idempotence, gzip)
│   ├── consumer.py         # BaseConsumer (manual offset commit, retry policy)
│   ├── dlq.py              # DeadLetterQueueService
│   └── events/             # Typed CloudEvents payload contracts
├── workers/
│   ├── main.py             # Standalone background worker entrypoint
│   └── consumer_runner.py  # WorkerSupervisor registering all consumers
├── api/
│   ├── crud_router/        # CRUDRouter: query_model.py + routes.py + search.py + router.py
│   ├── deps.py             # session → service dependency providers
│   └── v1/
│       ├── categories.py   # 100% generated
│       ├── products.py     # generated + custom endpoints
│       ├── employees.py
│       └── logs.py         # Elasticsearch log search endpoints
└── main.py                 # app factory, lifespan, exception handlers
alembic/                    # Migration environment + versioned revisions
deploy/                     # Dockerfile, nginx.conf, gunicorn.conf.py, sysctl.conf, docker-compose.yml, k8s/
tests/                      # API tests + unit tests (mocked repository)
```

---

<a id="-1-lakh-100k-concurrent-requests-system-execution-flow"></a>
## ⚡ 1 Lakh (100k) Concurrent Requests: System Execution Flow

<a id="visual-sequence-diagram-end-to-end-request-path"></a>
### Visual Sequence Diagram: End-to-End Request Path

The following diagram tracks an incoming request step-by-step through network sockets, kernel buffers, proxy rate-limiting, tracing middleware, crypto operations, database multiplexing, cache invalidation, and response generation:

<p align="center">
  <img src="assets/sequence_flow.png" alt="Visual Sequence Diagram: End-to-End Request Path" width="100%" />
</p>

---

<a id="phase-1-ingress-network-sockets--linux-kernel-tcp-tuning"></a>
### Phase 1: Ingress, Network Sockets & Linux Kernel TCP Tuning

At 100,000 concurrent sockets, default Linux kernel settings cause catastrophic connection drops (standard `somaxconn` is 128; file descriptor limits default to 1,024).

The kernel configuration in [`deploy/sysctl.conf`](deploy/sysctl.conf) tunes the OS network subsystem:

```ini
# 1. Backlog Queues: Prevent packet dropping under 100k connection bursts
net.core.somaxconn = 65535             # Listen queue depth for established TCP sockets awaiting accept()
net.ipv4.tcp_max_syn_backlog = 65535   # Half-open SYN queue depth (neutralizes SYN flood drops)
net.core.netdev_max_backlog = 65535    # NIC driver packet buffer before CPU processing

# 2. Ephemeral Ports & TIME_WAIT Socket Recycling
net.ipv4.ip_local_port_range = 1024 65535  # Expands outbound port range to 64,511 sockets
net.ipv4.tcp_tw_reuse = 1                  # Safely reuses TIME_WAIT sockets for new connections
net.ipv4.tcp_fin_timeout = 15              # Drops closing sockets after 15s instead of default 60s
net.ipv4.tcp_max_tw_buckets = 2000000      # Allows up to 2 million sockets in TIME_WAIT

# 3. Memory & Buffer Auto-Tuning
net.ipv4.tcp_rmem = 4096 87380 16777216   # Min 4KB, default 87KB, max 16MB receive window
net.ipv4.tcp_wmem = 4096 65536 16777216   # Min 4KB, default 65KB, max 16MB send window

# 4. File Descriptors & Memory Limits
fs.file-max = 2097152                     # OS-wide file descriptor limit
vm.max_map_count = 262144                 # High memory mapping count for Redis & multithreading
vm.overcommit_memory = 1                  # Guarantees Redis background snapshots don't fail
```

In addition, `/etc/security/limits.conf` sets `nofile 1048576`, enabling worker processes to hold tens of thousands of simultaneous open sockets without throwing `EMFILE (Too many open files)`.

---

<a id="phase-2-multi-host--multi-container-distribution-l4--nginx"></a>
### Phase 2: Multi-Host / Multi-Container Distribution (L4 & Nginx)

No single machine or process handles 100k concurrent requests. Traffic is distributed across tiers:

1. **Layer 4 Hardware Balancer (Anycast BGP / ECMP / HAProxy)**:
    - Distributes incoming TCP streams across **13 to 20 servers** using IP hash or round-robin without decrypting TLS or inspecting HTTP headers.
2. **Nginx Reverse Proxy Tier** ([`deploy/nginx.conf`](deploy/nginx.conf)):
    - **Asynchronous Event Loop**: `use epoll; worker_connections 65535; multi_accept on;` allows a single Nginx process to manage tens of thousands of client connections with negligible RAM overhead.
    - **Connection Exhaustion Defense**: `limit_conn conn_limit_per_ip 100;` prevents rogue clients or DDoS botnets from holding thousands of idle sockets open.
    - **Request Rate Limiting**: `limit_req zone=req_limit_per_ip burst=50 nodelay rate=100r/s;` smooths traffic bursts while throttling sustained abuse.
    - **Persistent Upstream Connection Pooling**:

        ```nginx
        upstream app_backend {
            server app:8000;
            keepalive 128; # Reuses persistent TCP sockets to FastAPI workers
        }
        ```

        By reusing open sockets, Nginx eliminates the 3-way TCP handshake (`SYN -> SYN-ACK -> ACK`) on every HTTP call, saving 5–15 ms per request.

---

<a id="phase-3-multi-process-asynchronous-worker-pool-gunicorn--uvicorn"></a>
### Phase 3: Multi-Process Asynchronous Worker Pool (Gunicorn + Uvicorn)

Configured in [`deploy/gunicorn.conf.py`](deploy/gunicorn.conf.py):

1. **Worker Sizing Formula**:
    - `workers = int(os.getenv("WEB_CONCURRENCY", multiprocessing.cpu_count()))` launches 1 worker per physical core.
    - On an 8-core host, 8 workers run in parallel. Across a 16-server cluster, **128 worker processes** run concurrently.
2. **`uvloop` + `httptools` C-Based Engine**:
    - Each worker runs Uvicorn with `uvloop`, a fast Cython wrapper over `libuv`.
    - `uvloop` replaces standard `asyncio` with native Linux `epoll`.
    - When an async handler awaits database or Redis I/O, the event loop switches instantly to other sockets without CPU context switching.
    - Each worker handles **~800 to 1,200 concurrent sockets** smoothly.
3. **Memory & Stability Protections**:
    - `max_requests = 10000` with `max_requests_jitter = 1000`: Workers automatically recycle after 10,000 requests with randomized jitter, eliminating memory leaks over weeks of uptime.
    - `worker_tmp_dir = "/dev/shm"`: Worker heartbeats are written to tmpfs (RAM), preventing worker timeout kills caused by disk I/O spikes.

---

<a id="phase-4-step-by-step-function-execution-call-stack-write-path-post-users"></a>
### Phase 4: Step-by-Step Function Execution Call Stack (Write Path: POST /users)

Here is the exact step-by-step function execution sequence when a client sends `POST /api/v1/users`:

```
1. Client sends HTTP POST payload to Nginx
   └─► Nginx checks limit_conn (100/IP) & limit_req (100r/s burst=50)
   └─► Nginx dispatches request over upstream keepalive TCP socket

2. Gunicorn worker receives socket on uvloop event loop
   └─► Uvicorn httptools parses HTTP headers into ASGI scope dictionary:
       scope = {"type": "http", "method": "POST", "path": "/api/v1/users",
                 "headers": [(b"host", b"..."), (b"x-forwarded-for", b"203.0.113.1"), ...]}
   └─► Dispatches to FastAPI ASGI application in app/main.py

3. app/main.py [L48]: request_id_and_tracing_middleware(request, call_next)
   └─► Extracts incoming "X-Request-ID" header or generates uuid.uuid4()
   └─► Attaches identifier to request.state.request_id = req_id
   └─► Passes control to route pipeline via await call_next(request)

4. app/services/redis_rate_limit.py [L82]: RateLimiter.__call__(request, response)
   └─► resolve_client_ip(request, settings.TRUSTED_PROXIES):
       • Validates direct TCP socket peer against TRUSTED_PROXIES (e.g. 127.0.0.1, 10.0.0.0/8)
       • Extracts real client IP from X-Forwarded-For (untrusted peers cannot spoof IP headers)
   └─► Resolves identity: "usr:<user_id>" (if JWT present) or "ip:<client_ip>"
   └─► Computes slot-pinned key: "rl:{users:ip:203.0.113.1}:1698403200"
   └─► Executes atomic Redis Lua script via EVALSHA:
       • redis.call('INCR', key) -> increments hit count
       • redis.call('EXPIRE', key, window) -> sets TTL only on first hit
   └─► Injects response headers: X-RateLimit-Limit, X-RateLimit-Remaining, X-RateLimit-Reset
   └─► (If quota exceeded, raises HTTPException -> HTTP 429)

5. app/core/database.py [L77]: get_db(request) (Dependency Injection)
   └─► session_factory = request.app.state.session_factory
   └─► async with session_factory() as session:
       • Opens isolated SQLAlchemy AsyncSession(bind=engine, expire_on_commit=False, autoflush=False)
       • Unit of work transaction begins

6. app/schemas/user.py [L22]: UserCreate validation (Pydantic v2)
   └─► validate_e164_phone(v):
       • Strips whitespace, dashes, and parentheses
       • Verifies regex ^\+[1-9]\d{1,14}$
       • Normalizes phone to strict E.164 (+14155552671)
   └─► Validates email format via EmailStr

7. app/api/v1/users.py [L8]: create_user(payload, service) via CRUDRouter
   └─► Thin HTTP controller receives validated UserCreate schema
   └─► Delegates directly to UserService: await service.create_user(payload)

8. app/services/user.py [L42]: create_user(payload)
   └─► Lifecycle hook: await self.before_create(payload)
   └─► Pre-generates UUID: user_id = uuid.uuid4()
   └─► Builds Associated Authenticated Data (AAD): aad = f"users.phone:{user_id}".encode("utf-8")

   ┌────────────────────────────────────────────────────────────────────────┐
   │ 8a. Cryptographic Encryption (app/core/security.py [L85]: CryptoService)│
   │ • Generates fresh 12-byte random nonce: nonce = os.urandom(12)         │
   │ • Resolves active cipher: cipher = self._ciphers[self._active_version] │
   │ • Encrypts with AES-256-GCM + computes Poly1305/GCM tag bound to AAD   │
   │ • Packs binary: [version(1B) || nonce(12B) || ciphertext+tag(N+16B)]   │
   │ • Encodes as URL-safe Base64 without padding                           │
   └────────────────────────────────────────────────────────────────────────┘

   ┌────────────────────────────────────────────────────────────────────────┐
   │ 8b. Deterministic Blind Indexing (app/core/security.py: CryptoService) │
   │ • Normalizes phone string                                              │
   │ • Computes HMAC-SHA256(blind_index_key, normalized_phone)              │
   │ • Generates 64-character hex digest stored in phone_blind_index column │
   └────────────────────────────────────────────────────────────────────────┘

   └─► Instantiates User(id=user_id, phone_encrypted=..., phone_blind_index=...)
   └─► Invokes Repository: await self.repo.create(user)

9. app/repositories/base.py [L95]: BaseRepository.create(entity)
   └─► self.session.add(entity)
   └─► await self.session.flush():
       • Sends INSERT INTO users (...) VALUES (...) to database driver
       • Driver dispatches query to PgBouncer transaction pool
       • PgBouncer attaches query to pre-warmed backend connection to PostgreSQL 16
       • PostgreSQL executes insert, enforcing UNIQUE constraints on email & phone_blind_index
       • Returns without committing transaction (unit of work remains open)

10. Cache Invalidation: await self.cache.invalidate_lists()
    └─► RedisCluster executes: INCR cache:users:list_ver
    └─► O(1) single-key atomic increment across Redis Cluster
    └─► All existing list cache keys (e.g. cache:users:list:v42:...) become orphaned instantly

11. Output Serialization: await self.to_read_schema(entity)
    └─► Decrypts phone using AAD tag verification
    └─► Masks PII: plain_phone -> "+1*******2671"
    └─► Returns UserRead schema

12. Transaction Finalization (app/core/database.py [L83]: get_db)
    └─► await session.commit() -> Commits transaction in PostgreSQL via PgBouncer
    └─► PgBouncer reclaims server connection immediately for next client
    └─► await session.close() returns connection to local pool

13. Response Generation (app/main.py)
    └─► ORJSONResponse encodes UserRead to binary JSON in C/Rust
    └─► Injects X-Request-ID header: response.headers["X-Request-ID"] = req_id
    └─► Returns HTTP 201 Created to Nginx -> Client
```

> **File & line index for this path**: [app/main.py:48](app/main.py) (tracing middleware) · [app/services/redis_rate_limit.py:82](app/services/redis_rate_limit.py) (`RateLimiter.__call__`) · [app/core/database.py:77,83](app/core/database.py) (`get_db`) · [app/schemas/user.py:22](app/schemas/user.py) (`UserCreate`) · [app/api/v1/users.py:8](app/api/v1/users.py) · [app/services/user.py:42](app/services/user.py) (`UserService.create_user`) · [app/core/security.py:85](app/core/security.py) (`CryptoService`) · [app/repositories/base.py:95](app/repositories/base.py) (`BaseRepository.create`).

---

<a id="phase-5-step-by-step-function-execution-call-stack-read-path-get-usersid"></a>
### Phase 5: Step-by-Step Function Execution Call Stack (Read Path: GET /users/{id})

<p align="center">
  <img src="assets/read_cache_flow.png" alt="Cache-Aside Read Path & Stampede Mutex Protection" width="100%" />
</p>

When 100,000 clients simultaneously request `GET /api/v1/users/{id}`, the cache-aside engine ensures PostgreSQL is completely protected from traffic spikes:

```
1. Request arrives at app/api/v1/users.py: get_user_by_id(user_id)
   └─► Delegates to UserService: await service.get_user_by_id(user_id)

2. app/services/base.py: get_by_id(user_id, cache_enabled)
   └─► Checks Redis cache: await self.cache.get_item(user_id)

   ┌────────────────────────────────────────────────────────────────────────┐
   │ CASE A: CACHE HIT (Over 95% of high-concurrency traffic)               │
   │ • Redis returns cached JSON payload in < 1 ms                          │
   │ • Deserializes directly into UserRead schema                           │
   │ • Zero database connections opened! PostgreSQL receives 0 queries.    │
   └────────────────────────────────────────────────────────────────────────┘

   ┌────────────────────────────────────────────────────────────────────────┐
   │ CASE B: NEGATIVE CACHE HIT (Attacker querying non-existent UUIDs)      │
   │ • Redis returns sentinel: {"__not_found__": true}                      │
   │ • Immediately raises NotFoundError (HTTP 404)                          │
   │ • Database is 100% shielded against random key scan attacks.           │
   └────────────────────────────────────────────────────────────────────────┘

   ┌────────────────────────────────────────────────────────────────────────┐
   │ CASE C: CACHE MISS — CACHE STAMPEDE MUTEX LOCK COORDINATION            │
   │ 1. Worker attempts to acquire distributed lock:                        │
   │    SET cache:users:lock:{user_id} "1" NX PX 5000                       │
   │                                                                        │
   │ 2a. WORKER A (Lock Acquired):                                          │
   │     • Queries database: await self.repo.get(user_id)                   │
   │     • If not found in DB: writes {"__not_found__": true} (TTL 30s)     │
   │     • If found: calls to_cache_dict() (masks phone)                    │
   │     • Writes to Redis with ±10% TTL jitter (e.g. 270-330s)             │
   │     • Releases lock: DEL cache:users:lock:{user_id}                    │
   │                                                                        │
   │ 2b. WORKERS B, C, ... N (Lock Busy):                                   │
   │     • Backs off and sleeps for 50 milliseconds                         │
   │     • Re-reads from Redis item cache: now populated by Worker A!       │
   │     • Result: 100,000 simultaneous misses cause EXACTLY 1 DB query!    │
   └────────────────────────────────────────────────────────────────────────┘
```

---

<a id="phase-6-high-concurrency-keyset-cursor-pagination-path"></a>
### Phase 6: High-Concurrency Keyset Cursor Pagination Path

Traditional SQL pagination using `OFFSET 50000 LIMIT 50` requires PostgreSQL to scan and discard 50,000 rows on every call, causing query execution times to balloon from 2ms to 2,500ms.

Our keyset pagination executes in $O(\log N)$ using composite B-Tree indexes:

```sql
-- Keyset cursor query executed by BaseRepository.list_keyset()
SELECT * FROM users
WHERE (created_at, id) < (:cursor_created_at, :cursor_id)
ORDER BY created_at DESC, id DESC
LIMIT 51; -- Fetches limit + 1 to determine if next page exists
```

1. **Zero Row Scanning Overhead**: PostgreSQL traverses directly to the leaf node in `ix_users_created_at_id` in $O(\log N)$ time, whether browsing Page 1 or Page 10,000.
2. **Stable Deterministic Sorting**: Combining timestamp with primary key UUID `(created_at, id)` guarantees 100% deterministic ordering even when thousands of users are created within the same millisecond.
3. **Cursor Token Wire Format**:

    ```
    Plaintext Cursor: "2026-10-28T09:00:00+00:00|f47ac10b-58cc-4372-a567-0e02b2c3d479"
    URL-Safe Base64:  "MjAyNi0xMC0yOFQwOTowMDowMCswMDowMHxmNDdhYzEwYi01OGNjLTQzNzItYTU2Ny0wZTAyYjJjM2Q0Nzk"
    ```

Client-facing usage: `GET /api/v1/users?limit=50&cursor=<token>` → [`app/schemas/base.py`](app/schemas/base.py) `decode_cursor()`/`encode_cursor()` handle the roundtrip, and [`app/repositories/base.py`](app/repositories/base.py) `list_keyset()` executes the seek query above. The result is cached under `cache:users:list:v{list_ver}:{filters_hash}` until the next mutation bumps `list_ver`.

---

<a id="phase-7-elasticsearch-search--async-logging-lifecycle"></a>
### Phase 7: Elasticsearch Search & Async Logging Lifecycle

The architecture implements **Command-Query Responsibility Segregation (CQRS)**: PostgreSQL is the authoritative system of record for all writes, while Elasticsearch 8 handles full-text fuzzy ranking, faceted search, and centralized high-throughput structured application logging. Full engine mechanics are in [§6 Deep Dive: Elasticsearch 8 Search & Structured Logging](#-deep-dive-elasticsearch-8-search--structured-logging).

#### CQRS Product Full-Text Search Path: `GET /api/v1/products/search?q=wireless`

```
[Client] ──► GET /api/v1/products/search?q=wireless&limit=20
   │
   ▼
[app/api/crud_router/search.py: add_search_route() -> CRUDRouter.search()]
   │ Extracts 'q', pagination parameters, and validates
   ▼
[app/services/product.py: ProductService.search(q="wireless", limit=20)]
   │
   ▼
[app/core/elasticsearch.py: ElasticsearchService.search()]
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

#### High-Throughput Non-Blocking Structured Log Ingestion

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

<a id="phase-8-relational--nested-query-execution-lifecycle"></a>
### Phase 8: Relational & Nested Query Execution Lifecycle

When querying complex nested structures (e.g. a `Product` and its associated `Category`), the async event loop imposes a hard constraint that this codebase enforces structurally.

#### The Async N+1 Dilemma & `lazy="raise"`

In traditional synchronous frameworks (like Django or sync SQLAlchemy), accessing `product.category_rel.name` automatically issues a hidden synchronous query behind the scenes. In an async event-loop architecture serving 100k concurrency, **implicit lazy loading blocks the entire async thread worker** and crashes with:

```
sqlalchemy.exc.MissingGreenlet: greenlet_spawn has not been called; can't call await_only() using greenlet_spawn
```

To eliminate this hazard permanently, all relationships in this codebase declare `lazy="raise"`. Every relational attribute access that is not explicitly pre-loaded immediately raises an error during testing — turning a silent $N+1$ production incident into an immediate test failure. The full model/schema/repository/service pattern for this is in [§9.10 Nested Tables & Entity Relationships Guide](#910-nested-tables--entity-relationships-guide-category-in-product).

#### Eager Loaded Nested Path: `GET /api/v1/products/{id}/detail`

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

<a id="-deep-dive-encryption-aad-key-rotation--blind-indexing"></a>
## 🔐 Deep Dive: Encryption, AAD, Key Rotation & Blind Indexing

Implemented in [`app/core/security.py`](app/core/security.py).

### 1. AES-256-GCM AEAD Wire Format

Plaintext phone numbers are never written to disk, never logged, and never placed in Redis cache. Each encrypted phone value is stored as a compact, self-contained binary token:

```
┌──────────────────┬──────────────────┬───────────────────────────────────────────┐
│ Version (1 Byte) │ Nonce (12 Bytes) │ Ciphertext + Authentication Tag (N+16 B)  │
└──────────────────┴──────────────────┴───────────────────────────────────────────┘
```

- **Nonce Security**: 12 random bytes generated via `os.urandom(12)` on _every single encryption_. Nonces are never reused.
- **Output Encoding**: URL-safe Base64 with padding stripped (`=`), safe for transport across URLs, JSON payloads, and headers without escaping.

### 2. Associated Authenticated Data (AAD) Protection

To prevent **ciphertext transplantation attacks** (where an attacker copies Alice's encrypted phone column into Bob's record in the database):

```python
aad = f"users.phone:{user_id}".encode("utf-8")
```

When encrypting, the 16-byte authentication tag is calculated over both the ciphertext and the user's specific `user_id`. If someone moves the ciphertext to a different user row, decryption raises `DecryptionError` because the AAD verification fails.

### 3. Zero-Downtime Key Rotation Protocol

The application supports seamless rotation of secret keys across millions of records without downtime:

- `ENCRYPTION_KEYS`: JSON mapping of versions to 32-byte hex keys: `{"1": "key1_hex...", "2": "key2_hex..."}`.
- `ACTIVE_KEY_VERSION`: Indicates which key writes new encryptions (e.g. `2`).
- **Decryption Process**: Inspects Byte 0 of the ciphertext payload. If Byte 0 is `1`, it retrieves Cipher 1. If Byte 0 is `2`, it retrieves Cipher 2. Older records decrypt normally while all new records write with the updated key.

### 4. HMAC-SHA256 Blind Indexing

Because AES-256-GCM uses random nonces, encrypting `+14155552671` twice produces completely different ciphertexts. This prevents using standard SQL `UNIQUE` constraints or `WHERE phone = :val` queries.

**The Blind Index Solution**:

- A dedicated 32-byte secret `BLIND_INDEX_KEY` is maintained separately from encryption keys.
- Normalized phone numbers are hashed using HMAC-SHA256:
  $$\text{Blind Index} = \text{HMAC-SHA256}(\text{BLIND\_INDEX\_KEY}, \text{Normalized Phone})$$
- Produces a deterministic 64-character hex string stored in the `phone_blind_index` column.
- PostgreSQL creates a `UNIQUE INDEX ix_users_phone_blind_index` on this column, providing $O(1)$ duplicate detection and instant exact-match queries without decrypting a single row.

### 5. PII Masking at Serialization Boundary

Plaintext phone numbers are never exposed in public schemas or cached dictionaries:

```python
def mask_phone(phone: str) -> str:
    # E.164: "+14155552671" -> "+1*******2671"
    if len(phone) <= 6:
        return "****"
    return phone[:2] + "*" * (len(phone) - 6) + phone[-4:]
```

---

<a id="-deep-dive-distributed-caching-stampede-locks--namespace-invalidation"></a>
## ⚡ Deep Dive: Distributed Caching, Stampede Locks & Namespace Invalidation

Implemented in [`app/services/base.py`](app/services/base.py) (`DynamicRedisCache`) and [`app/core/redis.py`](app/core/redis.py) (cluster client + distributed lock primitive).

### 1. Redis Cluster Slot Pinning

In Redis Cluster mode, keys are distributed across 16,384 hash slots. Multi-key operations fail with `CROSSSLOT Keys in request don't hash to the same slot` if keys reside on different shards.

- All rate-limit keys use hash tags: `rl:{<scope>:<identity>}:<window>`
- Everything inside `{...}` hashes to the exact same cluster node, ensuring 100% Redis Cluster compatibility without errors.
- Single-item entity keys (`cache:<prefix>:item:<id>`) deliberately carry **no** hash tag, so they spread uniformly across all master nodes via MurmurHash3 instead of concentrating hot keys on one shard.

### 2. O(1) Namespace Version Invalidation vs Set-Tracking

Traditional cache invalidation tracks query hashes in a Redis Set and executes multi-key `DEL` commands on data writes:

- ❌ **Set-Tracking**: Fails on Redis Cluster (`CROSSSLOT`), suffers from unbounded memory growth, and exhibits $O(N)$ latency spikes from scanning/deleting thousands of query keys (`DEL cache:user:list:*` or `KEYS cache:*` locks the single-threaded Redis engine).
- ✅ **Namespace Versioning (Our Approach)**:
    - Cache list keys include the namespace version: `cache:users:list:v{list_ver}:{filters_hash}`.
    - When any user is created, updated, or deleted, the service runs a single atomic command:

            ```
            INCR cache:users:list_ver
            ```

    - **Complexity**: $O(1)$ single-key atomic increment, taking microseconds.
    - **Cluster Compatibility**: 100% cluster safe — never multi-key.
    - **Memory Impact**: Zero. All existing query cache keys under the old version become instantly unreachable, and expire naturally via their standard TTL (`CACHE_LIST_TTL=120s`) without any wildcard scanning.

### 3. Cache Stampede Distributed Mutex Locks

When a hot item expires under 100k concurrency, thousands of workers would simultaneously miss cache and bombard PostgreSQL (the "Cache Stampede" or "Dogpile Effect").

- The first worker acquires a distributed mutex lock:

    ```redis
    SET cache:users:lock:{user_id} "1" NX PX 5000
    ```

- Other workers fail to acquire the lock, back off for 50ms, and re-read the cache entry once populated by the winning worker.
- Result: Exactly 1 database query is executed, regardless of how many thousands of requests missed simultaneously.

**Reference implementation** ([`app/core/redis.py`](app/core/redis.py)) — a reusable `DistributedLock` primitive with safe, token-verified release via Lua so a slow worker can never release a lock a newer worker now owns:

```python
class DistributedLock:
    def __init__(self, redis: Union[RedisCluster, Redis]) -> None:
        self.redis = redis

    async def acquire(self, lock_key: str, token: str, ttl_ms: int = 5000) -> bool:
        # Atomic SET NX PX: Acquire lock only if key does not exist
        return bool(await self.redis.set(lock_key, token, px=ttl_ms, nx=True))

    async def release(self, lock_key: str, token: str) -> bool:
        # Atomic Lua Script: Releases lock ONLY if the token matches the owner
        RELEASE_LOCK_LUA = """
        if redis.call("get", KEYS[1]) == ARGV[1] then
            return redis.call("del", KEYS[1])
        else
            return 0
        end
        """
        return bool(await self.redis.eval(RELEASE_LOCK_LUA, 1, lock_key, token))
```

### 4. Negative Caching Sentinel (`__not_found__`)

If an attacker queries thousands of non-existent UUIDs, each request would otherwise miss cache and hit the database — a classic cache-penetration DDoS vector.

- When an item is not found in PostgreSQL, the manager caches the sentinel defined as `NEGATIVE_CACHE_SENTINEL` in [`app/services/base.py`](app/services/base.py):

    ```json
    { "__not_found__": true }
    ```

    with a short 30-second TTL. Subsequent queries hit Redis and return 404 in < 1ms without touching the database.

### 5. Dynamic TTL Jitter & Per-Entity TTL Profiles

When thousands of items are cached simultaneously, expiring them at the exact same second creates a coordinated database traffic spike. Our cache automatically applies a **±10% randomized jitter** to all item TTLs, spreading expirations smoothly across time.

Granular per-entity TTL defaults are configured in [`app/core/config.py`](app/core/config.py):

- `CACHE_ITEM_TTL` (default 300s): individual entity records (e.g. a single user profile).
- `CACHE_LIST_TTL` (default 120s): paginated/list query results.
- `CACHE_NEGATIVE_TTL` (default 30s): the `__not_found__` sentinel.
- `CACHE_LOCK_TTL_MS` (default 5000ms): stampede-mutex lock expiry.

### 6. Dynamic Feature Flags & Per-Call Overrides

- Controlled globally via environment variables:

    ```ini
    CACHE_ENABLE_GET_ITEM=true
    CACHE_ENABLE_SET_ITEM=true
    CACHE_ENABLE_GET_LIST=true
    CACHE_ENABLE_INVALIDATE_LIST=true
    CACHE_ENABLE_STAMPEDE_LOCK=true
    CACHE_ENABLE_NEGATIVE_CACHE=true
    ```

- Or overridden per-call in code:

    ```python
    # Force bypass cache (e.g. admin screen or immediate post-mutation verify)
    user = await service.get_user_by_id(user_id, cache_enabled=False)
    ```

### 7. Request Idempotency Engine

Network dropouts between clients and reverse proxies can cause clients to retry write mutations (payment, order creation, reservations) that must not be double-applied.

- **Key Format**: `idempotency:{key}`
- **TTL**: 86,400s (24 hours)
- **Lifecycle**:
    1. Client sends header `X-Idempotency-Key: <UUID>`.
    2. First request executes `SET idempotency:<UUID> PENDING NX EX 86400`.
    3. If acquisition succeeds: Request executes business logic, then stores the final JSON response in `idempotency:<UUID>`.
    4. If acquisition fails: Returns the existing cached response, or HTTP 409 Conflict if the original request is still `PENDING`.

### 8. Verification Challenges & One-Time Tokens

For SMS verification codes, email magic links, or password reset challenges:

- **Key Format**: `challenge:{scope}:{token}` (e.g. `challenge:sms_verify:928374`)
- **TTL**: 300s (5 minutes)
- **One-Time Consumption**: Handled via a single-round-trip `GET` followed by an atomic `DEL`. Storing transient tokens in PostgreSQL would cause table bloat, autovacuum overhead, and write WAL amplification for data that is discarded within minutes.

### 9. Sliding-Window Rate Limiting & Fail-Open Resilience

- **Engine**: [`app/services/redis_rate_limit.py`](app/services/redis_rate_limit.py)
- **Key Format**: `rl:{<scope>:<identity>}:<window>` (hash-tagged for single-slot atomic Lua execution — see §1 above)
- **Execution**: Atomic `INCR` + `EXPIRE` (first hit only) executed via `EVALSHA` on request ingress.
- **Fail-Open Resilience**: If Redis is momentarily unreachable, the rate limiter logs a warning and allows traffic through rather than rejecting every request, protecting API availability over strict quota enforcement during a Redis outage.

### 10. Cluster High-Availability & Standalone Fallback

In [`app/core/redis.py`](app/core/redis.py):

- Connects across 16,384 hash slots on Redis 7 Cluster nodes (`redis-1:7000`, `redis-2:7001`, `redis-3:7002`).
- **Resilience**: If Cluster mode is inactive (e.g. local single-node development or maintenance mode), it catches the cluster-unavailable exception and automatically falls back to standalone Redis on the first reachable node, so local development never requires a real 3-node cluster.

---

<a id="-deep-dive-elasticsearch-8-search--structured-logging"></a>
## 🔎 Deep Dive: Elasticsearch 8 Search & Structured Logging

Elasticsearch provides distributed inverted indexing, BM25 scoring, and columnar Doc Values, decoupling analytical and full-text searches from PostgreSQL. Implemented in [`app/core/elasticsearch.py`](app/core/elasticsearch.py) and [`app/core/logging.py`](app/core/logging.py).

### 1. CQRS Read-Model Inverted Indexing

```
[Write Mutation] ──► PostgreSQL (Normalized ACID) ──► Asynchronous Indexing ──► Elasticsearch (Denormalized CQRS)
                                                                                       │
[Search Query]   ◄─────────────────────────────────────────────────────────────────────┘
```

When an entity (User, Category, Product) is created or updated:

1. PostgreSQL executes relational integrity checks and foreign keys.
2. A sanitized read DTO is constructed via the service's `to_cache_dict()` hook.
3. The document is indexed into `{index_prefix}-{entity}` (e.g. `demo-products`) via [`app/core/elasticsearch.py`](app/core/elasticsearch.py).
4. Full-text searches query Elasticsearch directly, bypassing the PostgreSQL connection pool entirely.
5. If Elasticsearch is disabled (`ELASTICSEARCH_ENABLED=false`) or unreachable, `NullElasticsearch` degrades every call to a no-op/empty result rather than raising, so primary CRUD traffic is never coupled to search-cluster health.

### 2. BM25 Full-Text Relevance Scoring & Fuzzy Matching

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

- **Typo Tolerance**: Levenshtein distance (`fuzziness: "AUTO"`) handles user misspellings without expensive regexes.

### 3. Columnar Doc Values & Real-Time Faceted Aggregations

E-commerce catalogs require dynamic category counts and price histograms:

- Elasticsearch uses **Doc Values** (disk-based columnar storage) to calculate aggregations across millions of matching documents in milliseconds.
- Resolves counts for filters (e.g., _Categories: Keyboards (120), Mice (45)_) in the same query as the search results.

### 4. Deep Pagination via `search_after`

Standard SQL `OFFSET 50000 LIMIT 20` requires database engines to scan and discard 50,000 rows. Elasticsearch instead uses `search_after` based on sort keys and document IDs:

```json
{
	"size": 20,
	"sort": [{ "created_at": "desc" }, { "_id": "asc" }],
	"search_after": [1730467200, "f81d4fae-7dec-11d0-a765-00a0c91e6bf6"]
}
```

This traverses millions of records with constant $O(1)$ retrieval cost — the same philosophy as the PostgreSQL keyset pagination in [Phase 6](#phase-6-high-concurrency-keyset-cursor-pagination-path), applied to the search index.

### 5. High-Throughput Non-Blocking Structured Logging

- **Engine**: [`app/core/logging.py`](app/core/logging.py) (`AsyncLogBuffer`, `AsyncElasticsearchLogHandler`)
- **Queue Mechanics**: Incoming log records are dispatched to a bounded in-memory ring buffer (`ELASTICSEARCH_LOG_BUFFER_SIZE`, default 1000).
- **Batching & Flushing**: A background task flushes logs in bulk HTTP requests (`_bulk` API) when either condition is met:
    - `ELASTICSEARCH_LOG_BATCH_SIZE` (default 100) records accumulate.
    - `ELASTICSEARCH_LOG_FLUSH_INTERVAL` (default 2.0s) elapses.
- **Backpressure Protection**: If Elasticsearch is congested and the buffer reaches capacity, debug-level logs are dropped first, ensuring HTTP request workers are never blocked waiting on the logging pipeline.
- **Log Index Lifecycle**: Log records are routed to `app-logs` timeseries indices with automated retention pruning (`ELASTICSEARCH_LOG_RETENTION_DAYS`, default 30 days).

### 6. Log Search API & Histogram Aggregations

Structured logs can be inspected programmatically via [`app/services/log_search.py`](app/services/log_search.py):

- Filter by `correlation_id`, `service`, `level` (ERROR, WARNING, INFO), and ISO 8601 timestamp range.
- Query aggregate error-rate histograms over time for operational observability via `GET /api/v1/logs/search?level=ERROR&time_range=24h`.

---

<a id="-deep-dive-apache-kafka-transactional-outbox--event-driven-architecture"></a>
## 📨 Deep Dive: Apache Kafka, Transactional Outbox & Event-Driven Architecture

Apache Kafka provides durable, distributed event streaming for inter-service communication, inventory reservation sagas, and asynchronous outbox dispatch, cooperating with PostgreSQL to sustain **100,000 concurrent requests** with strict data integrity and zero cross-process shared state.

> **Which module is actually running**: this codebase contains two consumer implementations. [`app/core/kafka.py`](app/core/kafka.py) (`consume_events`/`consume_events_forever`) is the one actually wired into [`app/main.py`](app/main.py)'s lifespan and [`app/workers/consumer_runner.py`](app/workers/consumer_runner.py) — it drives the live inventory-reservation consumer and implements the bounded exponential-backoff retry + dead-letter-queue escape described in §7.8 directly. The richer `BaseConsumer`/`RetryPolicy`/`DeadLetterQueueService` class hierarchy under [`app/kafka/`](app/kafka/) is a complete, independently tested, CloudEvents-typed alternative (covered in §9.11's developer guide) that is **not currently imported by the running app or worker** — treat it as a reference implementation to adopt deliberately (replacing the `app/core/kafka.py` call sites), not as code that is executing today.

```
┌───────────────────┬───────────────────┬────────────────────────┬───────────────────────┐
│ Tier / Engine     │ Storage Physics   │ Concurrency Model      │ Primary Responsibility│
├───────────────────┼───────────────────┼────────────────────────┼───────────────────────┤
│ PostgreSQL 16     │ Disk + MVCC       │ PgBouncer Multiplexed  │ ACID Source of Truth  │
│ Redis 7 Cluster   │ In-Memory RAM     │ Atomic Non-Blocking    │ Ephemeral State & Lock│
│ Elasticsearch 8   │ Lucene Inverted   │ Sharded Read Clustered │ CQRS Search & Logs    │
│ Apache Kafka      │ Append-Only Log   │ Partition Consumer Grp │ Event-Driven Streaming│
└───────────────────┴───────────────────┴────────────────────────┴───────────────────────┘
```

```
[Domain Mutation] ──► PostgreSQL Transaction ──► [outbox_events Table]
                            │ (Atomic Commit)
                            ▼
               [Outbox Dispatcher Daemon] ──► Kafka Broker (`domain.events`)
                                                    │
                                                    ▼
                                      [Microservice Consumers]
```

### 1. The Dual-Write Problem & Transactional Outbox Pattern

Directly publishing to Kafka inside an HTTP controller creates data inconsistencies:

```python
# ANTI-PATTERN: DO NOT DO THIS
async def create_order(order_data):
    await db.execute(insert(Order)...)         # If this succeeds...
    await kafka.publish("orders", order_data)  # ...but this fails, the event is permanently lost!
```

- If the database commit succeeds but the Kafka broker is unreachable, downstream services never hear about the order (silent data inconsistency).
- If Kafka publishing succeeds but the database transaction rolls back due to a constraint violation, downstream services process a phantom event that never actually happened.

**The Transactional Outbox Solution** ([`app/services/outbox.py`](app/services/outbox.py)): every domain event is written to the `outbox_events` table **inside the same PostgreSQL transaction** as the entity mutation. The event is guaranteed to persist if and only if the business data persists. A background dispatcher subsequently streams the event to Kafka.

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

### 2. Event Naming Convention Standard

All events conform to the standardized convention:
$$\mathbf{Event\ Type} = \texttt{<service\_name>.<model\_name>.<action\_name>[.<version>]}$$

```
HighPerfStatelessService . user . created [ . v1 ]
│                          │      │           │
├── Service Name (APP_NAME) │      │           └── Optional Version Tag
│                          └── Model (Lowercase)
│                                 └── Action (Lowercase)
```

- **`service_name`**: Automatically defaults to `APP_NAME` from `.env` via `get_settings().APP_NAME` (`HighPerfStatelessService`). Preserves original casing.
- **`model_name`**: Extracted directly from the SQLAlchemy model (`self.repo.model.__name__`, e.g., `User` -> `user`, `Category` -> `category`, `Product` -> `product`).
- **`action_name`**: Normalized lowercase action string (e.g., `created`, `updated`, `deleted`, `reservation_failed`).
- **`version` (Optional)**: If omitted or `None`, no version segment is appended (e.g. `HighPerfStatelessService.user.created`). If provided (e.g. `"v1"` or `"1"`), it's appended (e.g. `HighPerfStatelessService.user.created.v1`).

```json
{
	"event_id": "f81d4fae-7dec-11d0-a765-00a0c91e6bf6",
	"aggregate_id": "c9bf9e57-1685-4c89-bafb-ff5af830be8a",
	"event_type": "HighPerfStatelessService.user.created",
	"occurred_at": "2026-11-02T05:25:35.961836Z",
	"data": { "id": "c9bf9e57-1685-4c89-bafb-ff5af830be8a" }
}
```

### 3. Multi-Tiered Flag Controls (No Dynamic Triggers by Default)

To prevent bulk imports, migrations, and internal housekeeping updates from accidentally producing unwanted Kafka traffic, generic CRUD methods do **not** trigger outbox events dynamically by default:

```
                            [outbox_enabled: bool]
                            Master On/Off Switch
                                      │
                   ┌──────────────────┴──────────────────┐
                   ▼                                     ▼
                [False]                                [True]
         (All Outbox Bypassed)              (Outbox Capable Service)
                                                         │
                                    ┌────────────────────┴────────────────────┐
                                    ▼                                         ▼
                 [auto_outbox_events: False] (DEFAULT)      [auto_outbox_events: True]
                 Generic CRUD does NOT trigger events.      Generic CRUD auto-triggers.
                 Developer triggers explicitly.             Can suppress with False.
```

- **`outbox_enabled: bool = False`**: Master switch enabling/disabling outbox persistence.
- **`auto_outbox_events: bool = False` (Default)**: Disables dynamic outbox event creation during standard `create()`, `update()`, and `delete()` operations.
- **`emit_outbox_event: Optional[bool] = None`**: Per-method parameter enabling events only when explicitly requested (e.g. `service.create(payload, emit_outbox_event=True)`).
- **`enqueue_outbox_event()`** / **`self._enqueue_outbox_event()`**: Public and internal methods on `BaseService` to manually queue events for custom domain sagas.

#### Developer Triggering Scenarios

| Use Case                    | Method / Call                                                  | Resulting `event_type`                      |
| :-------------------------- | :------------------------------------------------------------- | :------------------------------------------ |
| **Standard CRUD (opt-in)**  | `service.create(payload, emit_outbox_event=True)`              | `HighPerfStatelessService.user.created`     |
| **Standard CRUD (default)** | `service.create(payload)`                                      | _No event generated_                        |
| **Custom action**           | `service.enqueue_outbox_event("verified", user)`               | `HighPerfStatelessService.user.verified`    |
| **Versioned action**        | `service.enqueue_outbox_event("verified", user, version="v1")` | `HighPerfStatelessService.user.verified.v1` |
| **Helper call**             | `format_event_type("order", "placed")`                         | `HighPerfStatelessService.order.placed`     |
| **Cross-service helper**    | `format_event_type("AuthService", "user", "created")`          | `AuthService.user.created`                  |

```python
# Scenario: specifying a contract version for a breaking schema change
user_service.enqueue_outbox_event(
    action="registered",
    entity=user,
    version="v2",
    extra_data={"schema_version": 2},
)
# Generated event_type: "HighPerfStatelessService.user.registered.v2"
```

A full step-by-step walkthrough of building a new event end to end (contract, triggering patterns, consumer, idempotency, DLQ) lives in [§9.11 Building Service-to-Service Events with Kafka](#911-building-service-to-service-events-with-kafka).

### 4. PostgreSQL Schema, High-Throughput Indexing & Sequential Safety

The `outbox_events` table ([`app/models/outbox.py`](app/models/outbox.py)) is designed for non-blocking concurrent polling:

```sql
CREATE TABLE outbox_events (
    id UUID PRIMARY KEY,
    topic VARCHAR(255) NOT NULL,
    event_key VARCHAR(255) NOT NULL,       -- Partition key (Aggregate ID, e.g. user_id)
    event_type VARCHAR(255) NOT NULL,      -- "HighPerfStatelessService.user.created"
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    published_at TIMESTAMPTZ,              -- NULL when pending, populated on publish
    attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT
);

-- Fast scan of unpublished events:
CREATE INDEX ix_outbox_events_unpublished
ON outbox_events (published_at, created_at);

-- FIFO sequential ordering protection per aggregate:
CREATE INDEX ix_outbox_events_aggregate_pending
ON outbox_events (topic, event_key, published_at, created_at);
```

**Non-blocking worker concurrency (`SKIP LOCKED`)** — the background dispatcher polls pending records using PostgreSQL row-level locks:

```sql
SELECT * FROM outbox_events AS o
WHERE o.published_at IS NULL
  AND NOT EXISTS (
      SELECT 1 FROM outbox_events AS older
      WHERE older.topic = o.topic
        AND older.event_key = o.event_key
        AND older.published_at IS NULL
        AND older.created_at < o.created_at
  )
ORDER BY o.created_at, o.id
LIMIT 100
FOR UPDATE SKIP LOCKED;
```

- **`SKIP LOCKED`**: Multiple worker instances poll concurrently without lock contention or deadlocks.
- **`NOT EXISTS`**: Guarantees strict FIFO ordering per aggregate — if Event 1 fails, Event 2 for the same entity waits until Event 1 succeeds.

### 5. Kafka Partitioning & Downstream Deduplication

1. **Partition keying**: all events are produced to Kafka using `key=event.event_key` (the entity/aggregate ID). Kafka guarantees that messages with an identical key land on the same partition, guaranteeing **strict in-order delivery**.
2. **At-least-once deduplication**: because network retries can resend events, downstream consumers must deduplicate using `event_id`:

```python
async def handle_user_created(event: dict) -> None:
    event_id = event["event_id"]
    # Atomic Redis check-and-set with 24-hour TTL:
    if not await redis.set(f"processed_event:{event_id}", "1", nx=True, ex=86400):
        return  # Duplicate event skipped safely

    user_id = event["data"]["id"]
    await send_welcome_email(user_id)
```

### 6. Producer Architecture

[`app/kafka/producer.py`](app/kafka/producer.py) — driver: `aiokafka.AIOKafkaProducer`.

- `acks="all"`: ensures acknowledgment by the full broker quorum (leader and in-sync replicas).
- `enable_idempotence=True`: eliminates duplicate messages from producer-side retries.
- `compression_type="gzip"`: minimizes network bandwidth overhead.

### 7. Consumer Architecture & Strict Manual Offset Commit

[`app/kafka/consumer.py`](app/kafka/consumer.py) — **the golden rule: database transaction commits first, Kafka offset commits second.**

- `enable_auto_commit=False`: offsets are never advanced automatically.
- **Failure recovery**: if the database transaction or handler fails, the consumer seeks back to the failed record offset (`consumer.seek(partition, failed_offset)`) and does **not** advance the offset, ensuring no event is ever skipped.

### 8. Consumer Retry Policy & Dead Letter Queue

The live consumer path, [`app/core/kafka.py`](app/core/kafka.py)'s `consume_events()`, retries any exception that escapes the handler up to `max_retries` (default 3) times in place, with exponential backoff (`base_backoff_seconds * 2**attempt`, default base 2.0s) before re-polling the same offset — so a transient failure (a DB hiccup, a momentary broker blip) gets several spaced-out chances to succeed without permanently stalling the partition, and without that backoff sleep blocking any *other* partition's messages in the same batch. If every retry is exhausted, and the caller supplied `producer`/`dlq_topic` (as both [`app/main.py`](app/main.py) and [`app/workers/consumer_runner.py`](app/workers/consumer_runner.py) do for the inventory-reservation consumer, using `KAFKA_INVENTORY_DLQ_TOPIC`), the message is dead-lettered with diagnostic headers (`x-error-message`, `x-original-topic`, `x-original-partition`, `x-original-offset`, `x-retry-count`, `x-failed-at`) and its offset is committed so the partition keeps moving; without a DLQ configured, it retries indefinitely rather than silently dropping the message. Handlers are still expected to classify their *own* permanently-invalid events (bad schema, unsupported event type) and dead-letter those themselves without raising — [`handle_inventory_reserved()`](app/services/inventory_reservations.py) does exactly that for `ValidationError`/`ValueError` — so this generic retry/DLQ layer is the last-resort safety net for everything else (unexpected exceptions a handler didn't anticipate), not the primary classification point.

The separate [`app/kafka/dlq.py`](app/kafka/dlq.py) `DeadLetterQueueService` + `BaseConsumer`/`RetryPolicy` hierarchy implements the same transient-vs-permanent retry/DLQ philosophy as a reusable class-based abstraction with jittered backoff, but — per the note at the top of this section — it isn't imported by the code that actually runs today.

### 9. Inventory Reservation Saga & TTL Sweeper

Manages high-throughput inventory allocation without race conditions ([`app/services/inventory_reservations.py`](app/services/inventory_reservations.py)):

1. Consumes `inventory.reserved.v1` events on `KAFKA_INVENTORY_TOPIC`.
2. **PostgreSQL conditional hold**: `products.stock` remains physical on-hand stock; `products.reserved_quantity` tracks holds; available-to-promise is `stock - reserved_quantity`.
3. If stock is sufficient, records the reservation with an expiration timestamp (`ttl_minutes`).
4. If stock is insufficient, emits `inventory.reservation_failed.v1` via the outbox.
5. **TTL sweeper daemon**: periodically scans expired reservations, releases `reserved_quantity`, and publishes `inventory.reservation_expired.v1`.

Invalid payloads and unknown products are routed to `KAFKA_INVENTORY_DLQ_TOPIC` (the latter logged at critical severity). This consumer currently handles reservation and TTL expiry; finalization/cancellation lifecycle events need their own event contract and handlers before they can safely adjust on-hand inventory.

### 10. Standalone Background Worker Daemon

[`app/workers/main.py`](app/workers/main.py) decouples long-running Kafka event consumption and outbox dispatch from web worker processes:

```bash
python -m app.workers.main
```

Manages supervisor lifecycle, graceful shutdown on `SIGINT`/`SIGTERM`, and asynchronous consumer loops outside Gunicorn. See [`app/workers/consumer_runner.py`](app/workers/consumer_runner.py) `WorkerSupervisor` for how consumers are registered — also covered in [§9.11.8](#911-building-service-to-service-events-with-kafka).

---

<a id="-deep-dive-database-connection-multiplexing-pgbouncer"></a>
## 🗄️ Deep Dive: Database Connection Multiplexing (PgBouncer)

### 1. The Process vs Async Sockets Reality

PostgreSQL operates on a process-per-connection model. Each connection forks an OS process consuming 5–10 MB of RAM, and CPU context switching degrades database performance rapidly when connection counts exceed 200–300.
Conversely, 128 async FastAPI workers across 16 servers can easily open thousands of concurrent sockets. Connecting them directly to PostgreSQL would crash the database engine.

### 2. PgBouncer Transaction Pooling Mode

Configured in [`deploy/docker-compose.yml`](deploy/docker-compose.yml):

```
[ 128 FastAPI Workers across 16 Servers ]
         │
         │ (1,280 Client Sockets: pool_size=5, max_overflow=5)
         ▼
[ PgBouncer Layer (Transaction Mode) ]
• max_client_conn = 2000
• default_pool_size = 80
         │
         │ (80 Pre-Warmed Backend Sockets)
         ▼
[ PostgreSQL 16 Primary Engine ]
• max_connections = 150 (30 reserved for superuser / maintenance)
```

In **Transaction Pooling Mode**, PgBouncer assigns a server connection to a client only for the exact duration of a transaction (`BEGIN ... COMMIT`). The moment `session.commit()` completes, PgBouncer immediately reclaims the physical backend socket and assigns it to another waiting worker.

### 3. Why `statement_cache_size = 0` is Mandatory

In Transaction Mode, PgBouncer reassigns backend server connections between transactions. `asyncpg` by default caches prepared statements by name on the client connection. If Transaction 1 prepares `_asyncpg_stmt_1` on Backend A, and Transaction 2 executes on Backend B, PostgreSQL crashes with:

```
prepared statement "_asyncpg_stmt_1" does not exist
```

We configure [`app/core/database.py`](app/core/database.py):

```python
connect_args={
    "statement_cache_size": 0,
    "prepared_statement_cache_size": 0,
}
```

This forces `asyncpg` to use direct execution without prepared statement caching, guaranteeing 100% stability across PgBouncer transaction pools.

### 4. Connection Budget Sizing Formula

$$\text{Total App Sockets} = \text{Nodes} \times \text{Workers/Node} \times (\text{pool\_size} + \text{max\_overflow}) \le \text{PgBouncer } \text{MAX\_CLIENT\_CONN}$$
$$16 \times 8 \times (5 + 5) = 1,280 \le 2,000 \quad \checkmark$$

$$\text{PgBouncer } \text{DEFAULT\_POOL\_SIZE} \le \text{PostgreSQL } \text{max\_connections} - \text{Headroom}$$
$$80 \le 150 - 30 = 120 \quad \checkmark$$

---

<a id="-developer-implementation-workflow-step-by-step-guide"></a>
## 🛠️ Developer Implementation Workflow (Step-by-Step Guide)

This section is a complete, copy-pasteable reference for developers creating a new entity, CRUD operations, relationships, or Kafka events in the project.

<a id="91-file-modification-checklist"></a>
### 9.1 File Modification Checklist

When adding a new entity (for example, `Order`), follow this checklist:

| Action     | File Path                              | Purpose                                                                      |
| ---------- | -------------------------------------- | ---------------------------------------------------------------------------- |
| **CREATE** | `app/models/order.py`                  | SQLAlchemy 2.0 Declarative Table Model                                       |
| **CREATE** | `app/schemas/order.py`                 | Pydantic v2 Schemas (`OrderCreate`, `OrderUpdate`, `OrderRead`, `OrderPage`) |
| **CREATE** | `app/repositories/order_repository.py` | Subclass of `BaseRepository[Order, uuid.UUID]`                               |
| **CREATE** | `app/services/order_service.py`        | Subclass of `BaseService` with domain logic and hooks                        |
| **CREATE** | `app/api/v1/orders.py`                 | Dynamic `CRUDRouter` with zero explicit boilerplate                          |
| **CREATE** | `tests/test_orders.py`                 | Automated tests (CRUD, pagination, validation, 404s)                         |
| **MODIFY** | `app/models/__init__.py`               | Export `Order` model for discovery                                           |
| **MODIFY** | `app/schemas/__init__.py`              | Re-export schemas                                                            |
| **MODIFY** | `app/repositories/__init__.py`         | Re-export repository                                                         |
| **MODIFY** | `app/services/__init__.py`             | Re-export service                                                            |
| **MODIFY** | `app/api/v1/router.py`                 | Mount `orders.router` under `/orders`                                        |

<a id="92-how-developers-add-a-new-entity-order-example"></a>
### 9.2 How Developers Add a New Entity (Order Example)

To demonstrate how the generic architecture eliminates boilerplate, here is the complete code to implement an `Order` entity with full CRUD, keyset pagination, caching, lifecycle hooks, and a custom domain action — end to end in about five minutes.

#### Step 1: Model (`app/models/order.py`)

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

**Key requirements**:

1. Inherit from `app.models.base.Base`.
2. Always define the composite index: `Index("ix_<table_name>_created_at_id", "created_at", "id")`.
3. Use `UUID(as_uuid=True)` for native PostgreSQL UUIDs.

---

#### Step 2: Schemas (`app/schemas/order.py`)

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

#### Step 3: Repository (`app/repositories/order_repository.py`)

```python
"""Order repository inheriting generic CRUD and keyset pagination primitives."""

from typing import List
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
    # - get(id) / get_by(**kwargs) / exists_by(**kwargs)
    # - create(entity) / update(entity, data) / delete(entity)
    # - list(**filters) / count(**filters) / list_keyset(limit, cursor_dt, cursor_id, **filters)
    # - bulk_create(items) / delete_where(**filters) / aggregate(func_name, column_name, **filters)
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

#### Step 4: Service (`app/services/order_service.py`)

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

#### Step 5: Dynamic Router (`app/api/v1/orders.py`)

Using `CRUDRouter`, standard CRUD (`POST /`, `GET /`, `GET /{item_id}`, `PUT /{item_id}`, `PATCH /{item_id}`, `DELETE /{item_id}`, `GET /search`) with keyset cursor pagination, dynamic Redis Cluster caching, and validation is generated dynamically with zero boilerplate. Custom domain endpoints attach directly to the same router:

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

#### Step 6: Register the Route & Exports

1. **Mount in master router** (`app/api/v1/router.py`):

    ```python
    from app.api.v1.categories import router as categories_router
    from app.api.v1.health import router as health_router
    from app.api.v1.orders import router as orders_router
    from app.api.v1.products import router as products_router
    from app.api.v1.users import router as users_router

    api_v1_router = APIRouter()
    api_v1_router.include_router(users_router)
    api_v1_router.include_router(products_router)
    api_v1_router.include_router(categories_router)
    api_v1_router.include_router(orders_router)  # <-- Mounts /orders CRUD + custom actions
    api_v1_router.include_router(health_router)
    ```

2. **Re-export in package inits**:
    - `app/models/__init__.py`: add `from app.models.order import Order`
    - `app/schemas/__init__.py`: add `from app.schemas.order import OrderCreate, OrderRead, OrderUpdate, OrderPage`
    - `app/repositories/__init__.py`: add `from app.repositories.order_repository import OrderRepository`
    - `app/services/__init__.py`: add `from app.services.order_service import OrderService`

The new entity is immediately operational with keyset pagination, Redis caching, stampede locks, and transaction management — with zero duplicated CRUD code.

---

<a id="93-using-baserepository-primitives"></a>
### 9.3 Using BaseRepository Primitives

[`BaseRepository[ModelType, IdType]`](app/repositories/base.py) implements the abstract [`IBaseRepository`](app/repositories/interface.py) interface and provides a **full dynamic query, filtering, and mutation engine**:

```python
# 1. Dynamic Exact & Operator-Based Filtering (Rich Lookups)
# Supports: eq, ne, gt, gte, lt, lte, in, notin, like, ilike, contains, icontains, startswith, endswith, isnull
products = await repo.list(
    category="electronics",
    price_cents__gte=5000,
    price_cents__lte=20000,
    status__in=["in_stock", "backorder"],
    name__ilike="%wireless%",
)

# 2. Dynamic Multi-Column Ordering (supports '-' prefix for descending)
users = await repo.list(order_by=["-created_at", "email"])

# 3. Dynamic Eager/Lazy Loading Options (joinedload, selectinload)
orders = await repo.list(options=[selectinload(Order.items)])

# 4. Dynamic Bulk Operations (Single network round-trip)
new_items = await repo.bulk_create([{"name": "Item 1"}, {"name": "Item 2"}])
deleted_count = await repo.delete_where(is_active=False, role="guest")

# 5. Dynamic SQL Aggregation (sum, avg, min, max, count)
total_revenue = await repo.aggregate("sum", "total_cents", status="completed")

# 6. Keyset Cursor Pagination (O(log N) B-Tree seek with composite index)
items, has_next = await repo.list_keyset(limit=50, cursor_dt=last_ts, cursor_id=last_id, role="admin")

# 7. Fast Existence & Count Checks
exists = await repo.exists_by(sku="KB-01", is_active=True)
active_count = await repo.count(is_active=True)
```

<a id="94-unit-of-work--multi-repository-transactions"></a>
### 9.4 Unit of Work & Multi-Repository Transactions

If a business action must modify multiple entities atomically (for example, creating an order and reducing inventory), both repositories must share the exact same `AsyncSession`:

**Pattern A — Shared `AsyncSession` via `get_db`** (preferred for request-scoped services):

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

        order = await self.order_repo.create(
            Order(user_id=user_id, total_cents=product.price_cents * quantity)
        )

        # Both changes were flushed (not committed).
        # When get_db exits, it executes ONE single commit for both tables atomically!
        return OrderRead.model_validate(order)
```

**Pattern B — Explicit multi-repository transaction with a shared session**:

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

**Golden Rules**:

- Never call `session.commit()` inside repositories or services' repository calls — only `get_db` (or a `uow` context manager) commits.
- Repositories only call `session.flush()`, pushing SQL to the transaction buffer without committing.
- This design allows a service to update multiple repositories within a single atomic database transaction.

<a id="95-the-nullcache--null-object-pattern"></a>
### 9.5 The NullCache / Null Object Pattern

[`BaseService`](app/services/base.py) uses [`NullCache`](app/services/interface.py) whenever no distributed cache is provided. This completely eliminates fragile `if self.cache is not None:` branches, ensuring clean polymorphic execution across cached and non-cached environments — every cache call (`get_item`, `set_item`, `invalidate_lists`, ...) is simply a safe no-op when `NullCache` is active.

<a id="96-overriding-service-lifecycle-hooks"></a>
### 9.6 Overriding Service Lifecycle Hooks

[`BaseService`](app/services/base.py) exposes seven lifecycle hooks — three `before_*` hooks (run prior to the repository write), three `after_*` hooks (run after the transaction commits), and two serialization hooks:

```python
class CustomService(BaseService[...]):
    async def before_create(self, payload: CreateSchema) -> Model:
        """Called before repository insertion. Generate custom IDs, hash passwords, or encrypt."""
        return Model(id=uuid.uuid4(), **payload.model_dump())

    async def before_update(self, entity: Model, payload: UpdateSchema) -> None:
        """Called before database flush. Apply partial updates via exclude_unset=True."""
        update_data = payload.model_dump(exclude_unset=True)
        for key, val in update_data.items():
            setattr(entity, key, val)

    async def before_delete(self, entity: Model) -> None:
        """Called before the repository delete. Enforce custom cascading rules."""
        pass

    async def after_create(self, entity: Model) -> None:
        """Called after the entity is created AND the transaction commits.
        Side effects here see a durably persisted entity — e.g. send a welcome email,
        schedule a follow-up job, write an audit log. Runs before the Elasticsearch sync."""
        pass

    async def after_update(self, entity: Model, payload: UpdateSchema) -> None:
        """Called after the entity is updated AND the transaction commits.
        `payload` is the original update DTO, in case the hook needs to know which
        fields were explicitly set (`payload.model_dump(exclude_unset=True)`)."""
        pass

    async def after_delete(self, entity: Model) -> None:
        """Called after the entity is deleted AND the transaction commits.
        `entity` reflects its last in-memory state prior to deletion — use it to
        clean up dependent resources or emit a deletion notification."""
        pass

    async def to_read_schema(self, entity: Model) -> ReadSchema:
        """Converts model to public read schema. Decrypt ciphertext or mask PII fields."""
        return self.read_schema_cls.model_validate(entity)

    async def to_cache_dict(self, entity: Model) -> dict:
        """Serializes entity for Redis caching. Ensure plaintext PII is never returned."""
        read_model = await self.to_read_schema(entity)
        return read_model.model_dump(mode="json")
```

All six `before_*`/`after_*` hooks default to a no-op (`before_create`/`before_update` instead have a sensible generic default implementation — see [`app/services/base.py`](app/services/base.py)); override only the ones a given domain service actually needs. The `after_*` hooks fire unconditionally once the surrounding `create`/`update`/`delete` call reaches that point — including when the service has no `db` session (e.g. in unit tests) — so they're always a reliable place to hook in side effects without duplicating `BaseService`'s transaction-commit logic.

<a id="97-controlling-transactions--unit-of-work-boundary"></a>
### 9.7 Controlling Transactions & Unit of Work Boundary

- **The Golden Rule**: Generic repositories **never call `session.commit()`**.
- `BaseRepository` methods only call `session.flush()`, pushing SQL statements to the database transaction buffer without committing.
- **Unit of Work Boundary**: Managed by [`get_db`](app/core/database.py):
    - If the endpoint completes without errors, `get_db` executes `await session.commit()`.
    - If any error or exception occurs, `get_db` executes `await session.rollback()`.
    - This design allows a service to update multiple repositories within a single atomic database transaction.

<a id="98-controlling-cache-dynamically-per-operation--per-call"></a>
### 9.8 Controlling Cache Dynamically (Per-Operation & Per-Call)

The caching layer can be toggled globally via environment variables or overridden per-call in code:

**Global environment toggles (`.env`)**:

```ini
CACHE_ENABLE_GET_ITEM=true
CACHE_ENABLE_SET_ITEM=true
CACHE_ENABLE_GET_LIST=true
CACHE_ENABLE_INVALIDATE_LIST=true
CACHE_ENABLE_STAMPEDE_LOCK=true
CACHE_ENABLE_NEGATIVE_CACHE=true
```

**Per-call runtime overrides (Python)**:

```python
# Standard request: honors global cache settings
user = await service.get_user_by_id(user_id)

# Force bypass cache (e.g. admin screen or immediate post-mutation verify)
user = await service.get_user_by_id(user_id, cache_enabled=False)

# Force use cache even if global flag is off
user = await service.get_user_by_id(user_id, cache_enabled=True)
```

<a id="99-writing-automated-tests-for-a-new-entity"></a>
### 9.9 Writing Automated Tests for a New Entity

Every new entity should have tests validating CRUD, caching, and error paths ([`tests/test_orders.py`](tests/test_orders.py)):

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

<a id="910-nested-tables--entity-relationships-guide-category-in-product"></a>
### 9.10 Nested Tables & Entity Relationships Guide (Category in Product)

This section details how to model, query, validate, cache, and migrate nested tables and relationships in this architecture, using the **Category ↔ Product** relationship as the canonical production reference.

<p align="center">
  <img src="assets/relational_nested_flow.png" alt="Relational Eager Loading Pipeline" width="100%" />
</p>

#### 9.10.1 The `lazy="raise"` Rule in Async SQLAlchemy

In an asynchronous high-concurrency architecture serving **100,000 concurrent requests**, implicit lazy loading is strictly prohibited:

1. **The event loop trap**: in synchronous SQLAlchemy, accessing `product.category_rel` triggers a blocking synchronous I/O query. In an `asyncio` event loop, this blocks the worker process or raises `sqlalchemy.exc.MissingGreenlet`.
2. **The $N+1$ database penalty**: in a list of 100 products, naive attribute access executes $1 + 100 = 101$ roundtrips to the database multiplexer, exhausting PgBouncer backend connections instantly.
3. **The architectural fix**: all relationship attributes in this repository declare `lazy="raise"`. If a developer forgets to eager-load a relationship in the repository, SQLAlchemy immediately raises an exception at the query boundary rather than silently leaking queries into production.

#### 9.10.2 Step 1: Model Definition with Foreign Keys & Relationships

Both sides of the relationship are mapped using SQLAlchemy 2.0 type-annotated columns:

**Parent entity: `Category`** (`app/models/category.py`):

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

**Child entity: `Product`** (`app/models/product.py`):

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
>
> - Always place `index=True` on Foreign Key columns. Without a B-Tree index on the foreign key, PostgreSQL must execute a full-table scan on the child table whenever the parent table is queried or modified.
> - Use `ondelete="RESTRICT"` for critical business hierarchies to prevent accidental cascading deletion of inventory or orders.

#### 9.10.3 Step 2: Nested Pydantic Schemas (Embedded DTOs & Validation)

To optimize payload sizes, separate standard flat CRUD schemas from enriched nested detail schemas (`app/schemas/product.py`):

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

#### 9.10.4 Step 3: Repository Eager Loading (`selectinload` vs `joinedload`)

When querying entities with relationships, instruct SQLAlchemy to pre-load relations in the repository query:

- **`selectinload` (recommended for 1:N collections and async N:1)**: executes two queries linked by primary keys:

    ```sql
    SELECT * FROM products WHERE id = :id;
    SELECT * FROM categories WHERE id IN (:category_id);
    ```

    Prevents Cartesian product duplicates across tables.

- **`joinedload` (recommended for strict 1:1 foreign keys)**: uses an SQL `LEFT OUTER JOIN`. Do not use `joinedload` on large collections with `limit`/`offset`, since SQL joins multiply row counts before slicing.

Implementation in `app/repositories/product.py`:

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

#### 9.10.5 Step 4: Service Layer Validation & Cross-Entity Cache Invalidation

The Service layer enforces cross-entity business rules and cache consistency across parent and child entities (`app/services/product.py`):

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

**Cache coordination between parent and child** — when a parent `Category` is updated or deactivated, invalidate the child's list cache too, so stale embedded category data never serves from a product list:

```python
# Inside CategoryService.update_by_id():
updated_category = await self.repo.update(...)

# Invalidate product list version cache so all product listings re-fetch updated category data:
await self.redis.incr("cache:products:list_ver")
```

#### 9.10.6 Step 5: Dynamic `CRUDRouter` & Custom Nested Endpoints

In `app/api/v1/products.py`, standard endpoints are generated by `CRUDRouter`, while relational query filters and detailed nested endpoints are configured with zero boilerplate:

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

#### 9.10.7 Step 6: Database Migrations with Alembic

Whenever foreign keys or relationship tables are added or modified:

1. **Generate the migration script**:

    ```bash
    alembic revision --autogenerate -m "add_category_fk_to_products"
    ```

2. **Inspect the generated migration file** — verify that it correctly includes:

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

3. **Verify schema health**:

    ```bash
    python3 .agents/agents/developer-agent/skills/db_migration_helper.py
    ```

4. **Apply migration to database**:

    ```bash
    alembic upgrade head
    ```

#### 9.10.8 Step 7: Testing Relationships & Preventing N+1 Regressions

Add automated test cases validating foreign key constraints and eager loading (`tests/test_products.py`):

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

<a id="911-building-service-to-service-events-with-kafka"></a>
### 9.11 Building Service-to-Service Events with Kafka

This walkthrough guides developers through implementing end-to-end asynchronous service-to-service communication using Apache Kafka and the Transactional Outbox Pattern (mechanics covered in [§7 Deep Dive: Apache Kafka](#-deep-dive-apache-kafka-transactional-outbox--event-driven-architecture)).

```
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│                        SERVICE-TO-SERVICE EVENT-DRIVEN FLOW                             │
├─────────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                         │
│   [Service A: Producer]                                                                 │
│      │                                                                                  │
│      ├── 1. ACID Transaction (PostgreSQL via PgBouncer)                                 │
│      │      ├── Insert Domain Mutation (e.g. orders table)                              │
│      │      └── Insert Outbox Record (outbox_events table)                              │
│      ▼                                                                                  │
│   [PostgreSQL 16 Engine] ◄─── Atomic Commit (Zero Phantom Events)                       │
│      │                                                                                  │
│      ▼ (Asynchronous Poll via SKIP LOCKED)                                              │
│   [Outbox Dispatcher Daemon]                                                            │
│      │                                                                                  │
│      ├── 2. Publish with acks=all, gzip, partition key=aggregate_id                     │
│      ▼                                                                                  │
│   [Apache Kafka Cluster] (Topic: orders.events)                                         │
│      │                                                                                  │
│      ▼ (Consumer Group Partition Assignment)                                            │
│   [Service B: Consumer Daemon] (`app.workers.main`)                                     │
│      │                                                                                  │
│      ├── 3. Strict Manual Commit Protocol:                                              │
│      │      ├── A. Validate Schema (CloudEvents envelope)                               │
│      │      ├── B. Check Idempotency (Deduplicate event_id)                             │
│      │      ├── C. Execute Downstream Business Mutation (e.g. inventory)                │
│      │      ├── D. Commit PostgreSQL Transaction FIRST                                  │
│      │      └── E. Commit Kafka Partition Offset SECOND                                 │
│      │                                                                                  │
│      └── 4. On Error:                                                                   │
│             ├── Transient Error  ──► Exponential Backoff Retry (Max 3)                  │
│             └── Permanent Error  ──► Escalate to Dead Letter Queue (`orders.events.DLQ`)│
└─────────────────────────────────────────────────────────────────────────────────────────┘
```

#### 9.11.1 Architecture Overview: Decoupled Choreography & Dual-Write Prevention

In microservice architectures, synchronous HTTP calls (`POST http://inventory-service/reserve`) between services cause tight coupling, cascading latency spikes, and distributed transaction failure when one service is down. See [§7.1](#-deep-dive-apache-kafka-transactional-outbox--event-driven-architecture) for the dual-write anti-pattern and the transactional outbox fix — every domain event is written to `outbox_events` **inside the same PostgreSQL transaction** as the entity mutation, and a background dispatcher streams it to Kafka afterward.

#### 9.11.2 Step 1: Define Typed CloudEvents Contracts (`app/kafka/events/`)

Never publish arbitrary dictionaries or raw domain models. Every event must adhere to a typed, versioned CloudEvents contract subclassing `BaseEvent[PayloadT]` (`app/kafka/events/base.py`).

Create a new contract file, `app/kafka/events/order_events.py`:

```python
"""Order domain event schemas for Kafka streaming."""

from typing import Optional
from pydantic import BaseModel, Field

from app.kafka.events.base import BaseEvent


class OrderPlacedPayload(BaseModel):
    order_id: str = Field(description="Unique Order UUID")
    user_id: str = Field(description="Customer User UUID")
    total_cents: int = Field(ge=0, description="Order total amount in integer cents")
    item_count: int = Field(ge=1, description="Number of line items")
    currency: str = Field(default="USD", max_length=3)


class OrderPlacedEvent(BaseEvent[OrderPlacedPayload]):
    """CloudEvents envelope for an order placed domain event."""

    event_type: str = Field(default="order.placed.v1")


class OrderCancelledPayload(BaseModel):
    order_id: str
    user_id: str
    reason: str


class OrderCancelledEvent(BaseEvent[OrderCancelledPayload]):
    """CloudEvents envelope for an order cancellation domain event."""

    event_type: str = Field(default="order.cancelled.v1")


__all__ = [
    "OrderPlacedPayload",
    "OrderPlacedEvent",
    "OrderCancelledPayload",
    "OrderCancelledEvent",
]
```

**Contract design rules**:

1. **Schema versioning**: suffix the `event_type` with the schema version (e.g. `.v1`, `.v2`).
2. **Minimal PII**: never include sensitive unencrypted PII (credit cards, raw phone numbers, passwords) in event payloads. Use entity IDs or masked values.
3. **Immutability**: event payloads represent historical facts that have already occurred.

#### 9.11.3 Step 2: Triggering Events in the Service Layer

Events are **never published directly to Kafka over the network inside an HTTP service method** (this causes data loss or phantom events if the database transaction fails). Instead, events are enqueued as rows in PostgreSQL's `outbox_events` table **in the same ACID transaction** as the business mutation. Developers trigger events using three standard patterns depending on complexity:

```
┌────────────────────────────────────────────────────────────────────────────────────────┐
│                        SERVICE LAYER EVENT TRIGGERING OPTIONS                          │
├────────────────────────────────┬───────────────────────────────┬───────────────────────┤
│ Pattern                        │ When to Use                   │ Code Invocation       │
├────────────────────────────────┼───────────────────────────────┼───────────────────────┤
│ 1. Automatic Lifecycle Events  │ Standard CRUD operations      │ Automatic in          │
│    (`BaseService` Built-in)    │ (create, update, delete)      │ `service.create()`,etc│
├────────────────────────────────┼───────────────────────────────┼───────────────────────┤
│ 2. Domain Action Helper        │ Custom business methods on    │ `self._enqueue_outbox_│
│    (`_enqueue_outbox_event`)   │ domain entities               │ event("action", item)`│
├────────────────────────────────┼───────────────────────────────┼───────────────────────┤
│ 3. Explicit CloudEvents Envelop│ Multi-service sagas, complex  │ Direct `session.add(  │
│    (`OutboxEvent` Direct Add)  │ payloads with custom DTOs     │ OutboxEvent(...))`    │
└────────────────────────────────┴───────────────────────────────┴───────────────────────┘
```

In `app/api/deps.py`, the FastAPI dependency provider dynamically inspects the service constructor. When `KAFKA_ENABLED=true` in `.env`, it automatically injects:

```python
if "outbox_enabled" in init_sig.parameters:
    kwargs["outbox_enabled"] = settings.KAFKA_ENABLED  # True when Kafka is enabled

if "outbox_topic" in init_sig.parameters:
    kwargs["outbox_topic"] = settings.KAFKA_EVENTS_TOPIC  # Default: "domain.events"
```

**Pattern 1 — Automatic lifecycle event triggering.** For any entity extending `BaseService` (like `UserService`, `ProductService`, or `CategoryService`), standard CRUD operations can trigger events without writing any extra code once `emit_outbox_event=True` is passed (or `auto_outbox_events=True` is set — see [§7.3](#-deep-dive-apache-kafka-transactional-outbox--event-driven-architecture)):

- `service.create(payload, emit_outbox_event=True)` ➔ emits `{resource_name}.created`
- `service.update_by_id(id, payload, emit_outbox_event=True)` ➔ emits `{resource_name}.updated`
- `service.delete_by_id(id, emit_outbox_event=True)` ➔ emits `{resource_name}.deleted`

**Pattern 2 — Domain action helper (`self._enqueue_outbox_event`).** For custom service methods that mutate state (adjusting inventory, toggling a status), call the inherited helper:

```python
# app/services/product.py
class ProductService(BaseService[Product, ...]):

    async def adjust_stock(self, product_id: uuid.UUID, delta: int) -> ProductRead:
        async with self.db.begin():
            # 1. Mutate domain entity
            product = await self.repo.adjust_stock(product_id, delta)
            if product is None:
                raise NotFoundError(f"Product {product_id} not found")

            # 2. Trigger custom domain event: "product.stock_adjusted.v1"
            self._enqueue_outbox_event("stock_adjusted", product)

        # 3. Atomic commit: Both stock update and outbox event commit together!
        return await self.to_read_model(product)
```

**Pattern 3 — Explicit CloudEvents triggering (`OutboxEvent` direct addition).** For inter-microservice workflows requiring rich, strongly-typed event contracts:

```python
import uuid
from sqlalchemy.ext.asyncio import AsyncSession

from app.kafka.events.order_events import OrderPlacedEvent, OrderPlacedPayload
from app.models.order import Order
from app.models.outbox import OutboxEvent

class OrderService:
    def __init__(self, session: AsyncSession, outbox_topic: str = "orders.events") -> None:
        self.session = session
        self.outbox_topic = outbox_topic

    async def place_order(
        self,
        user_id: uuid.UUID,
        total_cents: int,
        item_count: int,
        correlation_id: str | None = None,
    ) -> Order:
        order_id = uuid.uuid4()

        async with self.session.begin():
            # 1. Mutate Domain Entity in PostgreSQL
            order = Order(id=order_id, user_id=user_id, total_cents=total_cents, status="pending")
            self.session.add(order)
            await self.session.flush()

            # 2. Construct Typed CloudEvents Envelope
            event = OrderPlacedEvent(
                aggregate_id=str(order.id),
                correlation_id=correlation_id,
                payload=OrderPlacedPayload(
                    order_id=str(order.id),
                    user_id=str(order.user_id),
                    total_cents=order.total_cents,
                    item_count=item_count,
                ),
            )

            # 3. Append to Transactional Outbox in the SAME Transaction
            outbox_record = OutboxEvent(
                id=uuid.UUID(event.event_id),
                topic=self.outbox_topic,
                # CRITICAL: event_key MUST be the aggregate_id to guarantee Kafka partition ordering!
                event_key=str(order.id),
                event_type=event.event_type,
                payload=event.model_dump(mode="json"),
            )
            self.session.add(outbox_record)

        # Atomic Commit: If anything fails above, neither the order nor the outbox record is saved.
        return order
```

**Developer triggering scenarios** (the service-method and manual-trigger shapes you'll actually write):

```python
# A. Standard CRUD with opt-in event emission
user = await user_service.create(
    UserCreate(name="Alice", email="alice@example.com", phone="+1234567890"),
    emit_outbox_event=True,
)
# event_type = "HighPerfStatelessService.user.created"

# B. Explicit manual trigger for a custom business action
async def verify_user_email(self, user_id: uuid.UUID) -> UserRead:
    user = await self.repo.get(user_id)
    user.is_verified = True
    await self.repo.update(user)
    self.enqueue_outbox_event(
        action="verified", entity=user,
        extra_data={"verified_at": datetime.now(timezone.utc).isoformat()},
    )
    # event_type = "HighPerfStatelessService.user.verified"
    await self.db.commit()
    return await self.to_read_schema(user)

# C. Specifying a contract version for a breaking change
user_service.enqueue_outbox_event(action="registered", entity=user, version="v2")
# event_type = "HighPerfStatelessService.user.registered.v2"

# D. Standalone event-formatting helper
from app.services.base import format_event_type
format_event_type("order", "placed")                      # -> "HighPerfStatelessService.order.placed"
format_event_type("order", "placed", version="v1")          # -> "HighPerfStatelessService.order.placed.v1"
format_event_type("BillingService", "invoice", "paid")       # -> "BillingService.invoice.paid"

# E. Complete real-world router implementation
@router.post("/", response_model=UserRead, status_code=status.HTTP_201_CREATED)
async def register_user(payload: UserCreate, service: UserService = Depends(get_user_service)) -> UserRead:
    user = await service.create(payload)
    service.enqueue_outbox_event(action="registered", entity=user)
    return user
```

#### 9.11.4 Step 3: Asynchronous Outbox Dispatching Mechanics

`OutboxDispatcher` (`app/services/outbox.py`) automatically streams committed outbox records to Kafka in the background, polling with `SELECT ... FOR UPDATE SKIP LOCKED` and the strict FIFO-per-aggregate guarantee described in [§7.4](#-deep-dive-apache-kafka-transactional-outbox--event-driven-architecture). `skip_locked=True` lets multiple horizontal worker processes poll concurrently without duplicate publishing, and the Kafka record key (`event_key`) routes same-aggregate events to the same partition for in-order delivery.

#### 9.11.5 Step 4: Implementing the Consuming Service (`BaseConsumer`)

Downstream services consume events by subclassing `BaseConsumer` (`app/kafka/consumer.py`). Create a consumer, e.g. `app/workers/order_notification_consumer.py`:

```python
"""Downstream consumer handling order events for notifications and inventory."""

import logging
from typing import Any
from sqlalchemy.ext.asyncio import async_sessionmaker, AsyncSession

from app.kafka.consumer import BaseConsumer
from app.kafka.events.order_events import OrderPlacedEvent
from app.kafka.producer import KafkaProducerService

logger = logging.getLogger("app.workers.order_notification")


class OrderNotificationConsumer(BaseConsumer):
    topic = "orders.events"
    group_id = "notification-service-orders"

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        producer: KafkaProducerService,
    ) -> None:
        super().__init__(producer=producer)
        self.session_factory = session_factory

    async def process_event(
        self,
        event_id: str,
        payload: dict[str, Any],
        raw_message: Any,
    ) -> None:
        # 1. Validate incoming event against typed CloudEvents schema
        event = OrderPlacedEvent.model_validate(raw_message.value)
        order_id = event.payload.order_id

        # 2. Execute downstream business logic within an ACID session
        async with self.session_factory() as session:
            async with session.begin():
                # Step A: Check Idempotency (Deduplicate) — see 9.11.6 below

                # Step B: Execute Downstream Mutation
                logger.info(
                    "Processing OrderPlacedEvent [event_id: %s] for order %s (total: $%0.2f)",
                    event.event_id, order_id, event.payload.total_cents / 100.0,
                )
                # Downstream side-effects: Send email, reserve inventory, update analytics...

        # Note: The Kafka Partition Offset is committed automatically by BaseConsumer
        # ONLY AFTER process_event() returns successfully without raising an exception!
```

#### 9.11.6 Step 5: Implementing Consumer Idempotency (Deduplication)

Kafka provides **at-least-once delivery**. A network blip during offset commit can cause Kafka to re-deliver the same event, so downstream consumers **must be idempotent**.

**Pattern A — database unique constraint (recommended).** Maintain a `processed_events` table:

```sql
CREATE TABLE processed_events (
    event_id VARCHAR(64) NOT NULL,
    consumer_group VARCHAR(128) NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (event_id, consumer_group)
);
```

Check and record the event atomically inside the business transaction:

```python
from sqlalchemy.dialects.postgresql import insert

async with session.begin():
    stmt = (
        insert(ProcessedEvent)
        .values(event_id=event.event_id, consumer_group=self.group_id)
        .on_conflict_do_nothing(index_elements=["event_id", "consumer_group"])
    )
    result = await session.execute(stmt)
    if result.rowcount == 0:
        logger.warning("Event %s already processed; skipping duplicate.", event.event_id)
        return  # Deduplicated!

    # Execute business mutation...
```

**Pattern B — natural business state (conditional updates).** For an inventory hold:

```sql
UPDATE products
SET reserved_quantity = reserved_quantity + :qty
WHERE id = :product_id AND (stock - reserved_quantity) >= :qty;
```

#### 9.11.7 Step 6: Error Handling, Retries & Dead Letter Queue (DLQ)

`BaseConsumer` (`app/kafka/consumer.py`) differentiates **transient** and **permanent** failures (full mechanics in [§7.8](#-deep-dive-apache-kafka-transactional-outbox--event-driven-architecture)):

1. **Transient errors** (deadlock, connection timeout): retried up to `max_retries=3` with exponential backoff (`delay = 2.0 * (2 ** attempt)`). If retries succeed, the offset commits normally.
2. **Permanent errors** (`ValidationError`, malformed JSON, corrupted data): escalated immediately to `DeadLetterQueueService`, routed to `{topic}.DLQ` with diagnostic headers (`x-error-message`, `x-original-topic`, `x-failed-at`, `x-retry-count`). The offset commits, unblocking the partition for SREs to inspect and replay.

#### 9.11.8 Step 7: Registering Consumers in the Background Worker Daemon

Register all microservice consumers inside `WorkerSupervisor` (`app/workers/consumer_runner.py`):

```python
class WorkerSupervisor:
    async def run(self) -> None:
        settings = get_settings()
        if not settings.KAFKA_ENABLED:
            logger.warning("KAFKA_ENABLED=false. Worker will idle.")
            await self._stop_event.wait()
            return

        engine = create_db_engine()
        session_factory = create_session_factory(engine)
        producer = KafkaProducerService(settings=settings)
        await producer.start()

        # Instantiate consumers
        order_consumer = OrderNotificationConsumer(session_factory=session_factory, producer=producer)

        # Launch consumer tasks
        tasks = [asyncio.create_task(order_consumer.start())]

        logger.info("All background Kafka consumers started successfully.")
        await self._stop_event.wait()

        # Graceful shutdown
        await order_consumer.stop()
        await producer.stop()
        await engine.dispose()
```

Run it in development or production with:

```bash
python -m app.workers.main
```

#### 9.11.9 Step 8: Writing Automated Tests for Kafka Events

Add automated unit and integration tests (`tests/test_kafka_events.py`):

```python
import pytest
from app.kafka.events.order_events import OrderPlacedEvent, OrderPlacedPayload

def test_order_placed_event_serialization():
    payload = OrderPlacedPayload(
        order_id="11111111-1111-1111-1111-111111111111",
        user_id="22222222-2222-2222-2222-222222222222",
        total_cents=4999,
        item_count=2,
    )
    event = OrderPlacedEvent(aggregate_id=payload.order_id, payload=payload)

    # 1. Verify CloudEvents Schema Envelope
    data = event.model_dump(mode="json")
    assert data["event_type"] == "order.placed.v1"
    assert data["event_version"] == 1
    assert data["payload"]["total_cents"] == 4999
    assert data["payload"]["currency"] == "USD"

    # 2. Verify Deserialization Roundtrip
    parsed = OrderPlacedEvent.model_validate(data)
    assert parsed.payload.order_id == payload.order_id
    assert parsed.payload.item_count == 2
```

Integration test verifying the database outbox event staging (`tests/test_kafka_outbox.py`):

```python
import pytest
from sqlalchemy import select
from app.models.outbox import OutboxEvent
from app.models.product import Product
from app.repositories.product import ProductRepository
from app.services.product import ProductService

@pytest.mark.asyncio
async def test_service_triggers_outbox_event(async_session, mock_redis):
    # 1. Arrange: Initialize service with outbox enabled
    repo = ProductRepository(async_session)
    service = ProductService(
        repo=repo, cache=mock_redis, db=async_session,
        outbox_enabled=True, outbox_topic="domain.events",
    )

    # 2. Act: Trigger custom business action
    product = Product(name="Gaming Mouse", sku="MOU-01", price_cents=4999, stock=10)
    async_session.add(product)
    await async_session.commit()

    await service.adjust_stock(product.id, delta=-2)

    # 3. Assert: Verify the OutboxEvent row was added to PostgreSQL
    stmt = select(OutboxEvent).where(
        OutboxEvent.event_key == str(product.id),
        OutboxEvent.event_type == "product.stock_adjusted.v1",
    )
    result = await async_session.execute(stmt)
    outbox_event = result.scalar_one_or_none()

    assert outbox_event is not None
    assert outbox_event.topic == "domain.events"
    assert outbox_event.published_at is None  # Awaiting background dispatcher
```

---

<a id="-api-reference-with-full-examples"></a>
## 📡 API Reference with Full Examples

### Users Endpoints (`/api/v1/users`)

#### 1. Create User

- **Method**: `POST /api/v1/users`
- **Request Body**:

```json
{
	"name": "Alice Smith",
	"email": "alice@example.com",
	"phone": "+14155552671",
	"role": "admin"
}
```

- **Response (`201 Created`)**:

```json
{
	"id": "f47ac10b-58cc-4372-a567-0e02b2c3d479",
	"name": "Alice Smith",
	"email": "alice@example.com",
	"role": "admin",
	"phone_masked": "+1*******2671",
	"created_at": "2026-10-28T09:00:00+00:00"
}
```

#### 2. List Users (Keyset Cursor Pagination)

- **Method**: `GET /api/v1/users?limit=2&role=admin`
- **Response (`200 OK`)**:

```json
{
	"items": [
		{
			"id": "f47ac10b-58cc-4372-a567-0e02b2c3d479",
			"name": "Alice Smith",
			"email": "alice@example.com",
			"role": "admin",
			"phone_masked": "+1*******2671",
			"created_at": "2026-10-28T09:00:00+00:00"
		}
	],
	"next_cursor": "MjAyNi0xMC0yOFQwOTowMDowMCswMDowMHxmNDdhYzEwYi01OGNjLTQzNzItYTU2Ny0wZTAyYjJjM2Q0Nzk",
	"limit": 2
}
```

#### 3. Fetch Single User

- **Method**: `GET /api/v1/users/f47ac10b-58cc-4372-a567-0e02b2c3d479`
- **Response (`200 OK`)**:

```json
{
	"id": "f47ac10b-58cc-4372-a567-0e02b2c3d479",
	"name": "Alice Smith",
	"email": "alice@example.com",
	"role": "admin",
	"phone_masked": "+1*******2671",
	"created_at": "2026-10-28T09:00:00+00:00"
}
```

---

### Products Endpoints (`/api/v1/products`)

#### 1. Create Product

- **Method**: `POST /api/v1/products`
- **Request Body**:

```json
{
	"name": "Ergonomic Mechanical Keyboard",
	"sku": "KB-MECH-RGB-01",
	"price_cents": 14999,
	"category": "electronics",
	"is_active": true
}
```

- **Response (`201 Created`)**:

```json
{
	"id": "7c9e6679-7425-40de-944b-e07fc1f90ae7",
	"name": "Ergonomic Mechanical Keyboard",
	"sku": "KB-MECH-RGB-01",
	"price_cents": 14999,
	"category": "electronics",
	"is_active": true,
	"created_at": "2026-10-28T09:05:00+00:00"
}
```

#### 2. Full Product Detail (Eager-Loaded Category)

- **Method**: `GET /api/v1/products/7c9e6679-7425-40de-944b-e07fc1f90ae7/detail`
- **Response (`200 OK`)**: see [§9.10.6](#9106-step-5-dynamic-crudrouter--custom-nested-endpoints) for the nested `category_rel` shape.

#### 3. Full-Text Search (CQRS via Elasticsearch)

- **Method**: `GET /api/v1/products/search?q=wireless&limit=20`
- Degrades gracefully to an empty result set if Elasticsearch is disabled or unreachable — see [§6](#-deep-dive-elasticsearch-8-search--structured-logging).

---

### Health Checks (L4/Kubernetes Probes)

#### Liveness Probe

- **Method**: `GET /healthz`
- **Response (`200 OK`)**: `{"status": "healthy"}`
- Responds in $O(1)$ without querying dependencies.

#### Readiness Probe

- **Method**: `GET /readyz`
- **Response (`200 OK`)**:

```json
{
	"status": "ready",
	"components": {
		"database": true,
		"redis": true
	}
}
```

- Executes `SELECT 1` on PostgreSQL and `PING` on Redis Cluster. Returns `503 Service Unavailable` if either dependency is unhealthy.

---

<a id="-testing-strategy"></a>
## 🧪 Testing Strategy

The test suite runs with zero external dependencies by using in-memory async mocks:

```bash
pytest -v
```

### Coverage Breakdown

| Test Suite                  | File                                                                                                                                                                                                             | What is Validated                                                                                                 |
| --------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------- |
| **Repository & Service**    | [`test_repository.py`](tests/test_repository.py)                                                                                                                                                                 | BaseRepository CRUD, BaseService caching, per-call `cache_enabled=False` override, list cache invalidation        |
| **Catalog Domain**          | [`test_products.py`](tests/test_products.py)                                                                                                                                                                     | Product entity creation, keyset listing, 404 NotFoundError verification, category eager-loading & N+1 regressions |
| **User Domain**             | [`test_users.py`](tests/test_users.py)                                                                                                                                                                           | Strict E.164 phone regex validation, Base64 cursor encoding and decoding                                          |
| **Employee Domain**         | [`test_employees.py`](tests/test_employees.py)                                                                                                                                                                   | 1-to-1 User relationship CRUD, schema validation                                                                  |
| **Cryptography**            | [`test_encryption.py`](tests/test_encryption.py)                                                                                                                                                                 | AES-256-GCM roundtrip, fresh random nonces, AAD tamper detection, key rotation across versions, blind indexing    |
| **Rate Limiter**            | [`test_rate_limit.py`](tests/test_rate_limit.py)                                                                                                                                                                 | Trusted proxy IP resolution, Redis Lua script execution, 429 quota enforcement, fail-open resilience              |
| **Cache Engine**            | [`test_cache.py`](tests/test_cache.py) / [`test_redis_advanced.py`](tests/test_redis_advanced.py)                                                                                                                | Dynamic feature flags, negative caching sentinel, namespace versioning, stampede mutex locks                      |
| **CRUD Router & Search**    | [`test_crud_router_and_elasticsearch.py`](tests/test_crud_router_and_elasticsearch.py)                                                                                                                           | Dynamic filter/sort model generation, Elasticsearch search offload, graceful degradation                          |
| **Logging & Search**        | [`test_logging_and_elasticsearch.py`](tests/test_logging_and_elasticsearch.py)                                                                                                                                   | Structured log formatting, async log buffer, log search API                                                       |
| **Kafka Events & Outbox**   | [`test_kafka_events.py`](tests/test_kafka_events.py) / [`test_kafka_outbox.py`](tests/test_kafka_outbox.py)                                                                                                      | CloudEvents (de)serialization, transactional outbox staging                                                       |
| **Kafka Consumer**          | [`test_kafka_consumer.py`](tests/test_kafka_consumer.py)                                                                                                                                                         | Manual offset commit, retry/backoff, DLQ routing                                                                  |
| **Inventory Reservations**  | [`test_inventory_reservations.py`](tests/test_inventory_reservations.py)                                                                                                                                         | Conditional stock holds, TTL expiry sweeper, idempotent reservation handling                                      |
| **Database Init & Seeders** | [`test_database_initialization.py`](tests/test_database_initialization.py) / [`test_database_seeders.py`](tests/test_database_seeders.py) / [`test_database_seeders_cli.py`](tests/test_database_seeders_cli.py) | Table-creation diffing, topological seeder resolution, CLI behavior                                               |
| **API Route Registration**  | [`test_api_route_registration.py`](tests/test_api_route_registration.py)                                                                                                                                         | All versioned routers mount correctly under `/api/v1`                                                             |

---

<a id="-database-initialization-migrations--seeders"></a>
## 🗃️ Database Initialization, Migrations & Seeders

Automatic initialization is opt-in. `AUTO_MIGRATE`, `AUTO_CREATE_TABLES`, and `AUTO_SEED` all default to `false`. Startup checks database connectivity and reports missing registered tables; it does not generate migrations or create schema unless explicitly enabled. Set `MIGRATION_DATABASE_URL` to a direct database connection when applying migrations, especially when the application URL goes through PgBouncer.

| Variable                 | Default                      | Behavior                                                                                                                                                                                  |
| ------------------------ | ---------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `DATABASE_URL`           | project async PostgreSQL URL | Application connection URL. PostgreSQL uses `asyncpg`, MySQL uses `asyncmy`, and SQLite uses `aiosqlite`. Common `mysql+pymysql://...` URLs are normalized to the supported async driver. |
| `MIGRATION_DATABASE_URL` | unset                        | Optional direct URL used by Alembic and the migration CLI; falls back to `DATABASE_URL`.                                                                                                  |
| `ENVIRONMENT`            | `development`                | Environment label for deployment configuration.                                                                                                                                           |
| `AUTO_MIGRATE`           | `false`                      | Applies existing Alembic revisions at startup. Never generates revisions.                                                                                                                 |
| `AUTO_CREATE_TABLES`     | `false`                      | Creates only missing metadata tables for an unmanaged development/test database. It refuses to bypass an existing `alembic_version` history.                                              |
| `AUTO_SEED`              | `false`                      | Runs registered idempotent seeders after the schema is ready.                                                                                                                             |
| `SEED_CATEGORIES`        | `[]`                         | Optional JSON list of category names for the safe reference-data example seeder. Existing rows are not updated. No demo users/accounts or RBAC tables are seeded.                         |

Production deployments should use the existing one-shot Compose migration job or a dedicated deployment job rather than enabling `AUTO_MIGRATE` in each API worker. PostgreSQL and MySQL migrations acquire a database-level lock, so concurrent CLI/startup migration attempts serialize; the one-shot deployment job remains preferred. Automatic initialization never drops tables or downgrades migrations. `create_all()` only runs when explicitly enabled and is not a substitute for Alembic. A database bootstrapped with `create-tables` is unmanaged and has no Alembic revision recorded; do not later run migrations against it without reviewing and explicitly baselining that schema.

### Mechanics behind the CLI

- **Table inspection & diffing** ([`app/database/initialization.py`](app/database/initialization.py)): inspects existing tables via `sa.inspect(connection).get_table_names()`, diffs against SQLAlchemy metadata (`missing = [t for t in Base.metadata.tables.values() if t.name not in existing]`), and creates **only the missing tables** via `Base.metadata.create_all(tables=missing)`. If `alembic_version` already exists, this refuses to run — protecting migration history from schema drift.
- **Distributed migration advisory lock** ([`alembic/env.py`](alembic/env.py)): multiple Gunicorn workers booting simultaneously must not run concurrent schema migrations. Acquired via a PostgreSQL session advisory lock (`SELECT pg_advisory_lock(738291046);`), runs `alembic.command.upgrade(config, "head")`, and releases it in a `finally` block (`SELECT pg_advisory_unlock(738291046);`).
- **Idempotent seeder engine with topological dependency resolution** ([`app/database/seeders/base.py`](app/database/seeders/base.py), [`app/database/seeders/runner.py`](app/database/seeders/runner.py)): each seeder declares a `name` and prerequisite `dependencies`; the runner builds a directed dependency graph and sorts seeders so parents seed before children (e.g. `category_seeder` before `user_seeder`), raising `RuntimeError` on a detected cycle. Seeders verify existing records before inserting and never overwrite or mutate existing records. All seeders run in a single transaction, in dependency order, behind a database lock on PostgreSQL/MySQL; a failing seeder rolls back the entire seeding transaction.

```bash
# Inspect connectivity, tables, and migration state
python -m app.database status
python -m app.database check

# Create missing tables only for an unmanaged development/test schema
python -m app.database create-tables

# Apply existing migrations, create an autogenerate revision, or roll back explicitly
python -m app.database migrate
python -m app.database revision "add product index"
python -m app.database rollback
python -m app.database history

# List registered seeders, run all, or run one seeder and its dependencies
python -m app.database seed list
python -m app.database seed
python -m app.database seed category_seeder
python -m app.database seed run category_seeder
```

Equivalent direct Alembic commands are supported:

```bash
alembic revision --autogenerate -m "create users table"
alembic upgrade head
alembic downgrade -1
alembic current
alembic history
alembic revision -m "custom migration"
```

### Recommended production best practice (hybrid migration approach)

1. **Run autogenerate**: `alembic revision --autogenerate -m "your_change"` to let Alembic generate the initial DDL diff.
2. **Open and inspect**: inspect the file in `alembic/versions/` to verify constraint names (e.g. `uq_employees_user_id`), cascade rules (`ondelete="CASCADE"`), and pagination indexes (`(created_at, id)`), and clean sequential naming (e.g. `0005_create_employees_table.py`) matching repo conventions.
3. **Apply the migration**: `alembic upgrade head`.
4. **Verify schema state**: `alembic current` and `python -m app.database status`.

The included `category_seeder` reads `SEED_CATEGORIES`; it is empty by default and never overwrites existing categories. No development users, passwords, role/permission catalog, or demo accounts are inserted.

### Standalone seeder CLI

```bash
# List all registered seeders in topological dependency order
python -m app.database.seeders list

# Run all seeders
python -m app.database.seeders run

# Run a specific seeder (e.g. user_seeder, category_seeder)
python -m app.database.seeders run user_seeder
```

### Dedicated worker mode (worker-only deployment)

Run background consumers, inventory sweepers, and the outbox dispatcher outside web API pods:

```bash
python -m app.workers.main
```

If tables are absent, `python -m app.database check` and `status` print the missing table list and remediation commands. For Alembic-managed databases, always use `python -m app.database migrate`; `create-tables` intentionally refuses to alter migration history.

---

<a id="-environment--container-networking-reference"></a>
## 🧩 Environment & Container Networking Reference

### Host vs. Docker networking & port matrix

| Service             | Container Name       | Host Access Port    | Container Network Port | Internal Network Address |
| :------------------ | :------------------- | :------------------ | :--------------------- | :----------------------- |
| **PostgreSQL 16**   | `demo_postgres`      | `5432` _(external)_ | `5432`                 | `postgres:5432`          |
| **PgBouncer 1.21+** | `demo_pgbouncer`     | `6432`              | `5432`                 | `pgbouncer:5432`         |
| **Redis Node 1**    | `demo_redis_1`       | `7000`              | `7000`                 | `redis-1:7000`           |
| **Redis Node 2**    | `demo_redis_2`       | `7001`              | `7001`                 | `redis-2:7001`           |
| **Redis Node 3**    | `demo_redis_3`       | `7002`              | `7002`                 | `redis-3:7002`           |
| **Elasticsearch 8** | `demo_elasticsearch` | `9200`              | `9200`                 | `elasticsearch:9200`     |
| **Apache Kafka**    | `demo_kafka`         | _(Internal)_        | `9092`                 | `kafka:9092`             |
| **FastAPI App**     | `demo_app`           | `8000`              | `8000`                 | `app:8000`               |
| **Nginx Proxy**     | `demo_nginx`         | `80`                | `80`                   | `nginx:80`               |

### Environment variable dictionary (`.env`)

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
POSTGRES_PORT=5432              # Host access port to PostgreSQL engine directly
PGBOUNCER_PORT=6432             # Host access port to PgBouncer connection pooler
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
# Domain events adhere to the "<service name>.<event>.<event name>" convention in code
# without polluting .env files with individual topic names.
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

### Dynamic service URL assembly in Pydantic settings

In [`app/core/config.py`](app/core/config.py), the `Settings` class uses a Pydantic `@model_validator(mode="after")` to automatically assemble and synchronize connection URLs:

- If individual variables (`DB_USER`, `DB_PASSWORD`, `DB_HOST`, `DB_PORT`, `DB_NAME`) are supplied, `DATABASE_URL` is dynamically updated.
- If individual Redis parameters (`REDIS_HOST`, `REDIS_PORT`, `REDIS_PASSWORD`) are supplied and `REDIS_URL` is empty, `REDIS_URL` is automatically assembled.
- If `ELASTICSEARCH_HOST` and `ELASTICSEARCH_PORT` are supplied, `ELASTICSEARCH_URL` is assembled accordingly.
- This gives full flexibility between local single-variable shell overrides, containerized deployments, and standard `.env` configuration files.

---

<a id="-local-development--production-deployment-guide"></a>
## 🚀 Local Development & Production Deployment Guide

```
┌────────────────────────────────────────────────────────────────────────────────────────┐
│                        PRODUCTION HIGH-CONCURRENCY SERVER TOPOLOGY                     │
├────────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                        │
│   [Clients / Browsers / Mobile Apps]                                                   │
│      │                                                                                 │
│      ▼ (100,000+ Concurrent HTTPS Connections)                                         │
│   [Edge Load Balancer / Nginx Reverse Proxy] (Port 80/443)                             │
│      │                                                                                 │
│      ├── TCP Upstream Connection Pool (Keepalive: 60s)                                 │
│      ▼                                                                                 │
│   [App Server 1..M] (Gunicorn + UvicornWorker on Port 8000)                            │
│      ├── Worker Processes: 1 per CPU Core (uvloop + httptools)                         │
│      ├── Memory Limits: Recycled every 10k requests (Anti-Fragmentation)               │
│      ├── Heartbeats in RAM: `/dev/shm` (Zero Disk I/O)                                 │
│      │                                                                                 │
│      ├──► PgBouncer Transaction Pooler (Port 6432) ──► PostgreSQL 16 (Port 5432)       │
│      ├──► Redis 7 Cluster (Ports 7000-7002)                                            │
│      ├──► Elasticsearch 8 Cluster (Port 9200)                                          │
│      └──► Apache Kafka Cluster (Port 9092)                                             │
│                                                                                        │
│   [Dedicated Background Worker Server] (`python -m app.workers.main`)                  │
│      ├── Consumes Kafka Events (Inventory, Notifications, Domain Sagas)                │
│      └── Dispatches PostgreSQL Outbox Events to Kafka Topics                           │
└────────────────────────────────────────────────────────────────────────────────────────┘
```

### Option 1: Docker Compose (Full Local Cluster, Fastest Start)

Starts PostgreSQL 16, applies Alembic migrations, then starts PgBouncer (transaction mode), 3-node Redis 7 Cluster, Kafka, FastAPI (Gunicorn), Elasticsearch, and Nginx:

```bash
cd deploy
docker-compose up --build -d

# Verify container health
docker-compose ps
```

The one-shot `migrate` service creates or upgrades the PostgreSQL schema before the API starts. For manual migration workflows, run `alembic upgrade head` from the repository root with `MIGRATION_DATABASE_URL` configured.

### Option 2: Local Virtual Environment (Fast Dev Mode)

```bash
# 1. Create and activate virtual environment
python3 -m venv venv
source venv/bin/activate

# 2. Install pinned dependencies
pip install -r requirements.txt

# 3. Configure environment
cp .env.example .env

# 4. Run development server with live reload
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

---

### Production Option A: Containerized Server Deployment (Docker Compose)

The fastest and most consistent way to deploy on a Linux server is using Docker Compose.

**Step 1 — Server preparation & repository setup**:

```bash
# Clone the repository onto the server
git clone https://github.com/your-org/fastapi-repo-pattern.git /opt/fastapi-app
cd /opt/fastapi-app

# Prepare production environment file
cp .env.example .env
chmod 600 .env  # Restrict permissions so only owner can read secrets
```

**Step 2 — Configure production secrets in `.env`**: set hardened `DB_PASSWORD`, `REDIS_PASSWORD`, `ELASTICSEARCH_PASSWORD`; generate secure 32-byte (64 hex character) encryption keys:

```bash
# Generate AES-256-GCM encryption key
openssl rand -hex 32

# Generate HMAC-SHA256 blind index key
openssl rand -hex 32
```

Configure `TRUSTED_PROXIES` with your load balancer or Nginx private IP addresses.

**Step 3 — Launch services via Docker Compose**:

```bash
docker compose -f deploy/docker-compose.yml up -d
```

**Step 4 — Verify database health & run migrations**:

```bash
docker compose -f deploy/docker-compose.yml exec app python -m app.database status
docker compose -f deploy/docker-compose.yml exec app python -m app.database migrate
docker compose -f deploy/docker-compose.yml exec app python -m app.database.seeders run
```

**Step 5 — Scale web application workers**:

```bash
docker compose -f deploy/docker-compose.yml up -d --scale app=4
```

**Step 6 — Run the dedicated background Kafka worker daemon**:

```bash
docker compose -f deploy/docker-compose.yml exec -d app python -m app.workers.main
```

---

### Production Option B: Bare-Metal Deployment (Systemd + Gunicorn + Nginx)

For maximum performance on dedicated Linux hardware, deploy natively using Systemd services to eliminate container virtualization overhead.

#### Step 1: Linux kernel & socket OS tuning

By default, standard Linux operating systems limit socket backlogs to 128 and open file descriptors to 1024, causing `ECONNRESET`, `connection refused`, and `too many open files` errors under load. Apply the kernel tuning profile from [`deploy/sysctl.conf`](deploy/sysctl.conf) (full parameter breakdown in [Phase 1](#phase-1-ingress-network-sockets--linux-kernel-tcp-tuning)):

```bash
sudo cp deploy/sysctl.conf /etc/sysctl.d/99-high-concurrency.conf
sudo sysctl -p /etc/sysctl.d/99-high-concurrency.conf
```

Then edit `/etc/security/limits.conf` to allow worker processes to hold 1,000,000+ open descriptors:

```ini
* soft nofile 1048576
* hard nofile 1048576
root soft nofile 1048576
root hard nofile 1048576
```

#### Step 2: Create dedicated system user & virtual environment

```bash
# 1. Create dedicated system user
sudo useradd -r -m -s /bin/bash -d /opt/fastapi fastapi

# 2. Deploy codebase
sudo cp -r /path/to/fastapi-repo-pattern/* /opt/fastapi/
sudo chown -R fastapi:fastapi /opt/fastapi
cd /opt/fastapi

# 3. Create Python 3.12 virtual environment
sudo -u fastapi python3.12 -m venv /opt/fastapi/venv
sudo -u fastapi /opt/fastapi/venv/bin/pip install --upgrade pip
sudo -u fastapi /opt/fastapi/venv/bin/pip install -r requirements.txt
```

#### Step 3: Create the web service unit (`/etc/systemd/system/fastapi-app.service`)

```ini
[Unit]
Description=FastAPI High-Performance Production Web Service
After=network.target postgresql.service pgbouncer.service redis.service
Wants=network-online.target

[Service]
Type=notify
User=fastapi
Group=fastapi
WorkingDirectory=/opt/fastapi
EnvironmentFile=/opt/fastapi/.env

# Start Gunicorn with tuned production configuration
ExecStart=/opt/fastapi/venv/bin/gunicorn -c deploy/gunicorn.conf.py app.main:app

# Zero-downtime rolling reload on SIGHUP
ExecReload=/bin/kill -s HUP $MAINPID

# Process Lifecycle & Resilience
Restart=always
RestartSec=3
KillMode=mixed
TimeoutStopSec=30

# File Descriptors & Memory
LimitNOFILE=1048576
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
```

#### Step 4: Create the background worker unit (`/etc/systemd/system/fastapi-worker.service`)

```ini
[Unit]
Description=FastAPI Kafka Event Consumer & Outbox Worker Daemon
After=network.target fastapi-app.service
Wants=network-online.target

[Service]
Type=simple
User=fastapi
Group=fastapi
WorkingDirectory=/opt/fastapi
EnvironmentFile=/opt/fastapi/.env

# Execute standalone worker supervisor
ExecStart=/opt/fastapi/venv/bin/python -m app.workers.main

# Process Lifecycle
Restart=always
RestartSec=5
LimitNOFILE=1048576
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
```

#### Step 5: Configure the Nginx edge reverse proxy

```bash
sudo cp deploy/nginx.conf /etc/nginx/sites-available/fastapi.conf
sudo ln -s /etc/nginx/sites-available/fastapi.conf /etc/nginx/sites-enabled/
sudo nginx -t && sudo systemctl reload nginx
```

#### Step 6: Enable and start services

```bash
sudo systemctl daemon-reload

# Enable auto-start on boot
sudo systemctl enable fastapi-app
sudo systemctl enable fastapi-worker

# Start services
sudo systemctl start fastapi-app
sudo systemctl start fastapi-worker

# Verify status
sudo systemctl status fastapi-app
sudo systemctl status fastapi-worker
```

---

### Zero-Downtime Rolling Reloads & Worker Recycling

When deploying new code or updating `.env` secrets on a live server, client requests must not be dropped.

```bash
# Gracefully reload the live Gunicorn master process
sudo systemctl reload fastapi-app
# (or kill -HUP <GUNICORN_MASTER_PID>)
```

**What Gunicorn does under the hood**:

1. The Gunicorn master process receives `SIGHUP`.
2. It re-reads `deploy/gunicorn.conf.py` and application code, spawning a new generation of Uvicorn workers.
3. The new workers bind to the existing listening socket and warm up.
4. Once new workers report ready, the master sends `SIGTERM` to the old workers.
5. Old workers finish servicing existing in-flight HTTP requests (`graceful_timeout=30s`) and terminate.
6. **Result**: zero dropped packets, zero 502 Bad Gateway responses, and continuous 100k throughput during deployment.

**Worker recycling (anti-fragmentation defense)** — in [`deploy/gunicorn.conf.py`](deploy/gunicorn.conf.py): `max_requests = 10_000` with `max_requests_jitter = 1_000`. Workers automatically restart after processing ~10,000 requests to reclaim memory fragmentation and prevent slow Python memory leaks; the randomized jitter prevents all workers from restarting at the exact same second.

---

### Production Day-2 Management & Observability

**1. Health & liveness inspection**:

```bash
# Liveness Check (Kubernetes / Load Balancer ping)
curl -i http://localhost:8000/livez
# HTTP 200: {"status": "alive"}

# Readiness Check (Validates PostgreSQL, Redis, Elasticsearch, Kafka)
curl -i http://localhost:8000/readyz
# HTTP 200: {"status": "ready", "components": {"database": "connected", "redis": "connected", "elasticsearch": "connected", "kafka": "connected"}}
```

**2. Executing schema migrations in production**:

```bash
# 1. Inspect current migration version
python -m app.database status

# 2. Run pending migrations safely (protected by PostgreSQL advisory lock)
python -m app.database migrate
```

**3. Log stream monitoring**:

```bash
# Follow live application logs in Systemd journal
sudo journalctl -u fastapi-app -f --output=cat

# Follow live background worker logs
sudo journalctl -u fastapi-worker -f --output=cat
```

Structured JSON logs are automatically streamed to the Elasticsearch `app-logs` index for visualization in Kibana or querying via the Log Search API (see [§6.6](#-deep-dive-elasticsearch-8-search--structured-logging)).

**4. Monitoring the Kafka Dead Letter Queue (DLQ)**: when a consumer encounters a corrupted payload or unrecoverable error, the message is routed to `{topic}.DLQ` (e.g. `orders.events.DLQ`). SREs inspect the DLQ topic to diagnose failed events:

```bash
sudo cp deploy/nginx.conf /etc/nginx/sites-available/fastapi.conf
sudo ln -s /etc/nginx/sites-available/fastapi.conf /etc/nginx/sites-enabled/
sudo nginx -t && sudo systemctl reload nginx
```

#### Step 6: Enable and start services

```bash
sudo systemctl daemon-reload

# Enable auto-start on boot
sudo systemctl enable fastapi-app
sudo systemctl enable fastapi-worker

# Start services
sudo systemctl start fastapi-app
sudo systemctl start fastapi-worker

# Verify status
sudo systemctl status fastapi-app
sudo systemctl status fastapi-worker
```

---

### Zero-Downtime Rolling Reloads & Worker Recycling

When deploying new code or updating `.env` secrets on a live server, client requests must not be dropped.

```bash
# Gracefully reload the live Gunicorn master process
sudo systemctl reload fastapi-app
# (or kill -HUP <GUNICORN_MASTER_PID>)
```

**What Gunicorn does under the hood**:

1. The Gunicorn master process receives `SIGHUP`.
2. It re-reads `deploy/gunicorn.conf.py` and application code, spawning a new generation of Uvicorn workers.
3. The new workers bind to the existing listening socket and warm up.
4. Once new workers report ready, the master sends `SIGTERM` to the old workers.
5. Old workers finish servicing existing in-flight HTTP requests (`graceful_timeout=30s`) and terminate.
6. **Result**: zero dropped packets, zero 502 Bad Gateway responses, and continuous 100k throughput during deployment.

**Worker recycling (anti-fragmentation defense)** — in [`deploy/gunicorn.conf.py`](deploy/gunicorn.conf.py): `max_requests = 10_000` with `max_requests_jitter = 1_000`. Workers automatically restart after processing ~10,000 requests to reclaim memory fragmentation and prevent slow Python memory leaks; the randomized jitter prevents all workers from restarting at the exact same second.

---

### Production Day-2 Management & Observability

**1. Health & liveness inspection**:

```bash
# Liveness Check (Kubernetes / Load Balancer ping)
curl -i http://localhost:8000/livez
# HTTP 200: {"status": "alive"}

# Readiness Check (Validates PostgreSQL, Redis, Elasticsearch, Kafka)
curl -i http://localhost:8000/readyz
# HTTP 200: {"status": "ready", "components": {"database": "connected", "redis": "connected", "elasticsearch": "connected", "kafka": "connected"}}
```

**2. Executing schema migrations in production**:

```bash
# 1. Inspect current migration version
python -m app.database status

# 2. Run pending migrations safely (protected by PostgreSQL advisory lock)
python -m app.database migrate
```

**3. Log stream monitoring**:

```bash
# Follow live application logs in Systemd journal
sudo journalctl -u fastapi-app -f --output=cat

# Follow live background worker logs
sudo journalctl -u fastapi-worker -f --output=cat
```

Structured JSON logs are automatically streamed to the Elasticsearch `app-logs` index for visualization in Kibana or querying via the Log Search API (see [§6.6](#-deep-dive-elasticsearch-8-search--structured-logging)).

**4. Monitoring the Kafka Dead Letter Queue (DLQ)**: when a consumer encounters a corrupted payload or unrecoverable error, the message is routed to `{topic}.DLQ` (e.g. `orders.events.DLQ`). SREs inspect the DLQ topic to diagnose failed events:

```bash
docker compose -f deploy/docker-compose.yml exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic orders.events.DLQ \
  --from-beginning \
  --property print.headers=true
```

---

<a id="-why-this-stack-is-fast--technology-choices--best-practice-rationale"></a>
## ⚙️ Why This Stack Is Fast — Technology Choices & Best-Practice Rationale

Every section above describes *how* a piece of this architecture works. This section is the *why*: for each major technology choice, what specifically makes it fast, why it's considered the standard best practice over the obvious alternative, and exactly where in this codebase it's implemented — so the claim is checkable, not marketing copy.

| Technology / Pattern | Why it's fast | Why it's the best-practice choice (vs. the obvious alternative) | Where it's implemented |
| --- | --- | --- | --- |
| **uvloop + httptools** (ASGI event loop) | `uvloop` is a Cython binding over `libuv` (the same C event-loop library Node.js uses) — it replaces Python's pure-Python `asyncio` selector loop with a compiled one, cutting event-loop overhead roughly 2-4x on socket-heavy workloads. `httptools` is a C HTTP/1.1 parser, so request-line/header parsing never runs in the Python interpreter. | The obvious alternative is Uvicorn's default pure-Python loop + `h11` parser — correct, but every parse and every `await` scheduling decision costs interpreter overhead that compounds linearly with connection count. At 100k concurrent sockets that overhead is the difference between saturating one core and needing two. | [`deploy/gunicorn.conf.py`](deploy/gunicorn.conf.py) sets `worker_class = "uvicorn.workers.UvicornWorker"`, which auto-selects `uvloop`/`httptools` when installed; both are pinned in [`requirements.txt`](requirements.txt). |
| **orjson** (`ORJSONResponse`) | Rust-based serializer; benchmarks consistently 2-10x faster than the stdlib `json` module for typical API payloads, and serializes `datetime`/`UUID`/dataclasses natively without writing custom `JSONEncoder` subclasses. | FastAPI's default `JSONResponse` uses stdlib `json` plus `jsonable_encoder`, which walks the response object with reflection on every single request — correct for arbitrary types, but it's unnecessary tax when your schemas are Pydantic v2 models you already control. | `FastAPI(default_response_class=ORJSONResponse, ...)` in [`app/main.py`](app/main.py). |
| **asyncpg** (PostgreSQL driver) | Implements PostgreSQL's binary wire protocol directly in C/Cython rather than the text protocol most pure-Python drivers use; it's the fastest Python Postgres driver in nearly every published async benchmark, often 2-3x `psycopg` (async mode). | `psycopg`/`psycopg2` are the "safe default" most tutorials reach for, but they're built around the text protocol and (for `psycopg2`) synchronous I/O — wrong shape for an event loop serving thousands of concurrent requests per worker. | `postgresql+asyncpg://` in [`app/db/session.py`](app/db/session.py)'s `normalize_async_database_url()`. |
| **PgBouncer transaction pooling** | PostgreSQL forks an OS process per connection (~5-10MB RAM each); at 1,280 potential app-side sockets that's infeasible to hold open directly. PgBouncer multiplexes them onto ~80 pre-warmed backend connections, attaching a real Postgres connection to a client **only for the duration of one transaction**. | The alternative — each of 128 Uvicorn workers opening its own direct Postgres connections — would need `max_connections` in the thousands, which degrades Postgres's own scheduler and memory use long before 100k concurrency is reached. | [`deploy/pgbouncer.ini`](deploy/pgbouncer.ini) (`pool_mode = transaction`); `statement_cache_size=0`/`prepared_statement_cache_size=0` in [`app/db/session.py`](app/db/session.py) make `asyncpg` compatible with it (see §8). |
| **Bounded `QueuePool` sized via `DB_POOL_SIZE`/`DB_MAX_OVERFLOW`** (not `NullPool`) | Reuses a small number of already-open asyncpg sockets to PgBouncer across requests. Opening a fresh TCP connection plus the Postgres/asyncpg startup handshake on *every single request* (what `NullPool` would do) adds real, avoidable latency to each one. | **This was a real gap this audit found and fixed** — [`app/db/session.py`](app/db/session.py) previously used `NullPool` whenever `PGBOUNCER_ENABLED=True` (the documented default), silently discarding the `DB_POOL_SIZE(5)`/`DB_MAX_OVERFLOW(5)` settings the "Connection Sizing Formula" in [`AGENTS.md`](AGENTS.md) §2 is computed against. Fixed to use a real bounded pool in both branches; PgBouncer still does its own backend-side multiplexing regardless. | [`app/db/session.py`](app/db/session.py) `create_db_engine()`. |
| **Redis 7 Cluster cache-aside** (stampede lock, namespace versioning, negative cache, TTL jitter) | Every primitive here is $O(1)$: `GET`/`SET`/`INCR` are single-key RAM operations in the sub-millisecond range, versus a disk-backed PostgreSQL round-trip. The namespace-version trick turns "invalidate N cached list pages" into one atomic integer increment instead of an $O(N)$ `DEL` scan (which also fails outright on a real cluster via `CROSSSLOT`). | The obvious alternative — tracking cached query keys in a Set and `DEL`-ing them on write — doesn't survive contact with Redis Cluster's hash-slot sharding, and the naive "just cache it" approach without a stampede lock lets a single hot-key expiry turn into a thundering-herd database spike. | [`app/services/base.py`](app/services/base.py) `DynamicRedisCache`; distributed lock in [`app/core/redis.py`](app/core/redis.py) (see §5). |
| **Keyset (cursor) pagination with a composite `(created_at, id)` index** | A B-Tree index seek is $O(\log N)$ regardless of how deep the page is. `OFFSET 50000` forces PostgreSQL to scan and discard 50,000 rows before returning anything — latency that grows linearly with page depth. | **This audit found and fixed a real gap**: `CategoryService.list_keyset()` was seeking on `(created_at, id)` without the model declaring that composite index — every category list page silently fell back to a full sort instead of an index seek. `User`/`Product`/`Employee` already had it correctly. | [`app/models/category.py`](app/models/category.py) now declares `ix_categories_created_at_id` (migration [`0006_category_keyset_and_reservation_fk_index.py`](alembic/versions/0006_category_keyset_and_reservation_fk_index.py)); pattern explained in §9.10.1. |
| **Elasticsearch 8 CQRS search** | An inverted index + BM25 scoring answers "which documents contain these terms, ranked by relevance" in milliseconds across millions of documents. The equivalent in PostgreSQL — `ILIKE '%term%'` — can't use a B-Tree index at all and forces a sequential scan that gets slower as the table grows. | Bolting full-text search onto the transactional database couples search load to write-path latency and competes for the same connection pool. CQRS splits them: Postgres stays the fast, simple source of truth; Elasticsearch absorbs the expensive fuzzy/ranked queries on its own hardware. | [`app/core/elasticsearch.py`](app/core/elasticsearch.py); graceful `NullElasticsearch` degradation keeps primary CRUD unaffected if the search cluster is down (see §6). |
| **Transactional outbox + Kafka** (vs. publishing directly in the request path) | Decouples the HTTP response from any downstream fan-out — the client gets its `201 Created` the instant the Postgres transaction commits, not after some other service has acknowledged an event. `SKIP LOCKED` lets multiple dispatcher workers poll the same outbox table concurrently without blocking each other. | Publishing to Kafka directly inside the request handler creates the dual-write problem: if the DB commits but the broker call fails (or vice versa), downstream consumers either never hear about it or react to an event that never actually happened. The outbox row commits atomically with the business row, so the event's existence is exactly as durable as the data it describes. | [`app/services/outbox.py`](app/services/outbox.py) `OutboxDispatcher` (see §7.1, §7.4). |
| **Bounded retry + DLQ in the Kafka consumer** (not unbounded tight-loop retry) | A transient failure (a DB hiccup) gets a few spaced-out retries with exponential backoff instead of hammering the dependency in a zero-delay loop; a message that's genuinely unprocessable stops blocking the rest of the partition once it's dead-lettered, instead of wedging that partition forever. | **This audit found and fixed a real gap**: the live consumer path (`app/core/kafka.py`, actually wired into `app/main.py`/`app/workers/consumer_runner.py` — see the note in §7) previously caught any handler exception, seeked back to the same offset, and immediately retried with **zero backoff and no DLQ escape** — one bad message or one slow dependency call would spin that partition's CPU forever. A separate, more complete `BaseConsumer`/`DeadLetterQueueService` implementation already existed under `app/kafka/`, but it was never imported by the running code. | [`app/core/kafka.py`](app/core/kafka.py) `consume_events()` now retries up to `max_retries` with exponential backoff, then dead-letters to `dlq_topic` if one is configured (wired through in `app/main.py` and `app/workers/consumer_runner.py`; see §7.8). |
| **AES-256-GCM + HMAC-SHA256 blind index** (PII encryption) | AES-GCM is a single-pass authenticated cipher with hardware acceleration on virtually every modern CPU (AES-NI) — encryption and integrity verification happen in one operation, not two. The blind index turns an exact-match lookup on encrypted data into a normal indexed `WHERE column = :hash` query — $O(\log N)$ — instead of decrypting every row to compare it ($O(N)$, and only possible with a non-deterministic cipher's ciphertext being different every time anyway). | The naive alternative — a deterministic cipher so you *can* index the ciphertext directly — leaks that two rows share the same plaintext (classic chosen-plaintext weakness). AES-GCM's random nonce avoids that, and the separate blind index gets the lookup speed back without weakening the encryption. | [`app/core/security.py`](app/core/security.py) `CryptoService` (see §4). |
| **Gunicorn multi-process + Uvicorn worker model** | A single Python process is bound by the GIL to one CPU core regardless of how many `async def` coroutines it runs. Gunicorn's pre-fork model gives you one real OS process per core, so `N` cores actually means `N`x the throughput, not `N`x the concurrency on one core. | Running "just Uvicorn" directly uses one process — fine for development, a hard ceiling in production. `max_requests`/`max_requests_jitter` worker recycling also prevents the slow memory-fragmentation creep every long-running Python process eventually suffers, without every worker restarting in the same instant. | [`deploy/gunicorn.conf.py`](deploy/gunicorn.conf.py) (see §3, §6.4 for zero-downtime reloads). |
| **Nginx (`epoll`) as the reverse-proxy tier** | `epoll` scales $O(1)$ per *active* file descriptor, versus `select`/`poll`'s $O(N)$ scan across *all* watched descriptors — the difference between 65,535 idle keepalive connections costing nothing and costing a full scan every event loop tick. | Terminating 100k raw client connections directly in Python application workers wastes their CPU cycles on connection churn and slow-client buffering instead of business logic; Nginx absorbs that at the edge and hands the app tier only clean, buffered requests over a small pool of reused keepalive sockets. | [`deploy/nginx.conf`](deploy/nginx.conf) (see §2). |
| **Linux kernel socket tuning** (`sysctl.conf`) | Default kernel limits (`somaxconn=128`, 1024 open files) create an artificial ceiling far below what the hardware and application can actually sustain — connections get silently dropped at the OS level before the app ever sees them, which is a uniquely confusing failure mode to debug. | Raising `somaxconn`, enabling `tcp_tw_reuse`, and raising `fs.file-max`/`nofile` doesn't make anything faster per se — it removes false ceilings so the rest of this table's optimizations can actually be exercised at 100k scale. | [`deploy/sysctl.conf`](deploy/sysctl.conf) (see §1). |
| **Redis Lua (`EVALSHA`) rate limiting** | `EVALSHA` runs the whole increment-and-check as one atomic script server-side — one round trip, zero race condition between the `INCR` and the `EXPIRE`. The `SHA`-keyed variant avoids re-sending the script's source text on every single request once it's cached server-side. | The naive client-side approach — `GET` the counter, check it, `INCR` it in separate round trips — has a race window where two concurrent requests can both read "under limit" and both proceed, and costs 2-3 round trips instead of 1. | **This audit found and fixed a real gap**: `RateLimiter`/`rate_limiter()` in [`app/services/redis_rate_limit.py`](app/services/redis_rate_limit.py) was fully built and its Lua script preloaded at startup, but no router or endpoint actually depended on it — zero requests were being rate-limited. Now wired in globally for every `/api/v1/*` route in [`app/main.py`](app/main.py) (health/readiness probes are deliberately excluded so K8s liveness checks are never throttled). |
| **Null Object Pattern** (`NullCache`, `NullElasticsearch`) | Every call site in the hot path (`self.cache.get_item(...)`, `self.elasticsearch.index_document(...)`) is an unconditional method call, never an `if self.cache is not None:` branch — one fewer branch per cache/search operation, and it reads cleaner. | The obvious alternative, defensive `None`-checks scattered through every service method, is easy to forget in a new method and makes "caching disabled" a special case the rest of the code has to remember to handle. Here, "disabled" is just a different object implementing the same interface as a no-op — polymorphism absorbs the special case instead of `if` statements. | [`app/services/interface.py`](app/services/interface.py) `NullCache`; [`app/core/elasticsearch.py`](app/core/elasticsearch.py) `NullElasticsearch`. |

<a id="audit-fixes-applied-to-match-the-claims-below"></a>
### Audit Fixes Applied to Match the Claims Below

A full-codebase pass against every claim in the table above found these concrete gaps between documentation and running code, all fixed as part of this pass (not just documented around):

1. **DB connection pool was `NullPool`, not the documented bounded pool** — [`app/db/session.py`](app/db/session.py) used `NullPool` whenever `PGBOUNCER_ENABLED=True` (the default), discarding `DB_POOL_SIZE`/`DB_MAX_OVERFLOW` entirely and opening a fresh connection to PgBouncer on every request. Fixed to use a real `QueuePool` sized from those settings in both branches.
2. **`categories` table was missing its keyset-pagination index** — `CategoryService.list_keyset()` seeks on `(created_at, id)`, but [`app/models/category.py`](app/models/category.py) never declared that composite index, unlike every other paginated entity. Added, plus migration `0006`.
3. **`inventory_reservations.product_id` foreign key had no index** — contradicts this repo's own stated rule ("always index foreign keys"); every lookup/cascade check against `products` forced a full table scan. Added `index=True`, plus the same migration.
4. **`REDIS_SSL` was never applied to Redis Cluster connections** — only honored when assembling a standalone `REDIS_URL`; a deployment using `REDIS_CLUSTER_NODES` directly with `REDIS_SSL=true` silently got an unencrypted connection. Fixed in [`app/core/redis.py`](app/core/redis.py) (both the primary `RedisCluster` construction and the standalone-fallback path).
5. **The Lua rate limiter was never wired into any route** — fully implemented, Lua script preloaded at startup, but zero endpoints depended on it. Wired in globally for `/api/v1/*` in [`app/main.py`](app/main.py), with the dependency failing open/closed consistently (matching its existing Redis-outage policy) if the rate-limiting infra isn't initialized yet rather than raising an unrelated error.
6. **The live Kafka consumer retried failed messages in a zero-backoff infinite loop with no DLQ escape** — [`app/core/kafka.py`](app/core/kafka.py)'s `consume_events()` is the code actually wired into `app/main.py`/`app/workers/consumer_runner.py` (a separate, more complete `BaseConsumer`/`DeadLetterQueueService` hierarchy exists under `app/kafka/` but isn't imported anywhere outside its own package). Added bounded exponential-backoff retry and DLQ dead-lettering, wired through both call sites.
7. **Module docstring/import ordering bug** in [`app/services/inventory_reservations.py`](app/services/inventory_reservations.py) — an import was placed before the module docstring, silently discarding it as `__doc__`. Reordered.
8. **Documentation drift**: a diagnostic header name (`x-failed-timestamp` vs. the code's actual `x-failed-at`) was corrected in two places in this README, and the Kafka section now explicitly states which consumer module is live vs. which is an unwired reference implementation, so the retry/DLQ narrative matches what actually executes.

All 92 existing tests continued to pass after each fix (one test asserting the old `NullPool` behavior was updated to assert the corrected bounded-pool behavior instead, since that was the bug, not the test).
