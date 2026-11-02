# Architecture & Coding Rules

1. **Dependency Injection**:
   - In FastAPI endpoint handlers, inject services using `Annotated[Service, Depends(get_service)]`.
   - Never instantiate services or repositories inside endpoint functions.

2. **Template Method Inheritance**:
   - Every domain service must inherit from `BaseService[Model, CreateSchema, UpdateSchema, ReadSchema, IdType]`.
   - Implement `before_create` for validation, ID generation, and encryption.
   - Implement `before_update` for partial updates and re-encryption.
   - Implement `before_delete` for custom cascading rules prior to deletion.
   - Implement `after_create` / `after_update` / `after_delete` for post-commit side effects (notifications, audit logs, follow-up jobs) that must only run once the mutation is durably persisted. All three default to a no-op.
   - Implement `to_read_schema` for decryption and masking.
   - Implement `to_cache_dict` for Redis serialization.
   - Do not override `create`, `get`, `update`, `delete` unless altering the core transaction boundary.
   - Polymorphic cache handling via `NullCache` ensures seamless execution across cached and non-cached environments.

3. **Transaction Boundaries**:
   - Unit of work transaction lifecycle is managed at the request boundary (`get_db` commit on success, rollback on error).
   - Repositories must NEVER call `commit()` or `rollback()`. They only execute `await session.flush()`.
   - Always catch `IntegrityError` and handle domain violations cleanly.

4. **Database Connection Multiplexing (PgBouncer)**:
   - Must configure `statement_cache_size: 0` and `prepared_statement_cache_size: 0` in `connect_args` for `asyncpg`.
   - Never rely on server-level prepared statements or transaction-persisting session state.

5. **CQRS & Elasticsearch 8 Integration**:
   - PostgreSQL is the primary source of truth for all writes.
   - Elasticsearch asynchronously indexes documents for fast full-text fuzzy queries, faceted searches, and autocomplete.
   - Always provide graceful fallback (`NullElasticsearch`) when Elasticsearch is disabled or unreachable.

6. **Keyset Cursor Pagination**:
   - All high-volume list endpoints must use composite keyset seek `(created_at, id)` instead of deep SQL offset scanning.
   - Keyset queries run in $O(\log N)$ time, preventing database memory exhaustion.

7. **Exception Handling**:
   - Do not raise `fastapi.HTTPException` in services or repositories.
   - Raise explicit domain exceptions: `NotFoundError`, `ConflictError`, `BusinessRuleError`, `ValidationError`.
   - Map domain exceptions to HTTP status codes at the router layer or via global exception handlers.
