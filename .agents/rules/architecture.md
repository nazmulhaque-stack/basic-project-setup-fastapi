# Architecture & Coding Rules

1. **Dependency Injection**:
   - In FastAPI endpoint handlers, inject services using `Annotated[Service, Depends(get_service)]`.
   - Never instantiate services or repositories inside endpoint functions.

2. **Template Method Inheritance**:
   - Every domain service must inherit from `BaseService[Model, CreateSchema, UpdateSchema, ReadSchema, IdType]`.
   - Implement `before_create` for validation, ID generation, and encryption.
   - Implement `before_update` for partial updates and re-encryption.
   - Implement `to_read_schema` for decryption and masking.
   - Implement `to_cache_dict` for Redis serialization.
   - Do not override `create`, `get`, `update`, `delete` unless altering the core transaction boundary.

3. **Transaction Boundaries**:
   - Services execute `await self.db.commit()` on successful mutations and `await self.db.rollback()` on exceptions.
   - Repositories must NEVER call `commit()` or `rollback()`.
   - Always catch `IntegrityError` in services and re-raise as `ConflictError`.

4. **Exception Handling**:
   - Do not raise `fastapi.HTTPException` in services or repositories.
   - Raise explicit domain exceptions: `NotFoundError`, `ConflictError`, `ValidationError`.
   - Map domain exceptions to HTTP status codes at the router layer.
