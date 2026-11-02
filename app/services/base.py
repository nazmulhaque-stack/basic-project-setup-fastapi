"""Generic Base Service Layer (Template Method Pattern).

Split out of a combined cache+service module so this file holds exactly one
responsibility: domain CRUD orchestration. The Redis caching infrastructure it
depends on (`DynamicRedisCache`, `NEGATIVE_CACHE_SENTINEL`, `get_cache`) lives
in `app/services/cache.py` — neither module needs to change when the other does.

- Template Method Pattern: Generic CRUD workflow orchestration with transaction consistency boundaries.
- Decoupled from transport layer: Emits domain-level exceptions (NotFoundError, ConflictError).
- Pluggable Lifecycle Hooks: `before_create`, `before_update`, `before_delete` (pre-mutation),
  `after_create`, `after_update`, `after_delete` (post-commit side effects), and
  `to_read_schema`, `to_cache_dict` (serialization).
- Keyset (cursor) B-Tree pagination and offset pagination with safety limits.
- Polymorphic Caching: Uses NullCache transparently when no cache instance is provided.
"""

from collections.abc import Mapping
from datetime import datetime, timezone
import logging
import os
from typing import (
    Any,
    Dict,
    List,
    Optional,
    Sequence,
    Tuple,
    Type,
    TypeVar,
    Union,
)
import uuid

from pydantic import BaseModel
from sqlalchemy import Uuid
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import get_settings
from app.core.elasticsearch import IElasticsearchService, NullElasticsearch
from app.core.exceptions import ConflictError, NotFoundError
from app.models.base import Base
from app.models.outbox import OutboxEvent
from app.repositories.interface import IBaseRepository
from app.schemas.base import decode_cursor, encode_cursor
from app.services.cache import NEGATIVE_CACHE_SENTINEL
from app.services.interface import IBaseService, ICacheService, NullCache

logger = logging.getLogger(__name__)

ModelType = TypeVar("ModelType", bound=Base)
CreateSchemaType = TypeVar("CreateSchemaType", bound=BaseModel)
UpdateSchemaType = TypeVar("UpdateSchemaType", bound=BaseModel)
ReadSchemaType = TypeVar("ReadSchemaType", bound=BaseModel)
IdType = TypeVar("IdType")


def format_event_type(
    *args: str,
    service: Optional[str] = None,
    resource: Optional[str] = None,
    model: Optional[str] = None,
    action: Optional[str] = None,
    version: Optional[str] = None,
) -> str:
    """Format and validate a standardized event_type conforming to:
    '<service_name>.<model_name>.<action_name>[.<version>]'

    Convention:
    - service_name: Defaults to settings.APP_NAME (e.g. 'HighPerfStatelessService').
    - model_name: Domain entity/model name, normalized to lowercase (e.g. 'user', 'category').
    - action_name: Domain action, normalized to lowercase (e.g. 'created', 'updated', 'deleted').
    - version: Optional version tag (e.g. 'v1'). Appended only if explicitly specified.

    Examples:
        format_event_type("user", "created") -> "HighPerfStatelessService.user.created"
        format_event_type("user", "created", "v1") -> "HighPerfStatelessService.user.created.v1"
        format_event_type("user", "created", version="v1") -> "HighPerfStatelessService.user.created.v1"
        format_event_type("HighPerfStatelessService", "user", "created") -> "HighPerfStatelessService.user.created"
        format_event_type("AuthService", "user", "created") -> "AuthService.user.created"
    """
    srv: Optional[str] = service
    res: Optional[str] = model or resource
    act: Optional[str] = action
    ver: Optional[str] = version

    if len(args) == 1:
        res = args[0]
    elif len(args) == 2:
        res, act = args[0], args[1]
    elif len(args) == 3:
        arg2_clean = str(args[2]).strip().lower()
        is_version = arg2_clean.isdigit() or (
            arg2_clean.startswith("v")
            and len(arg2_clean) > 1
            and (arg2_clean[1:].isdigit() or "." in arg2_clean)
        )
        if is_version:
            res, act, ver = args[0], args[1], args[2]
        else:
            srv, res, act = args[0], args[1], args[2]
    elif len(args) >= 4:
        srv, res, act, ver = args[0], args[1], args[2], args[3]

    if srv is None:
        try:
            from app.core.config import get_settings
            srv = get_settings().APP_NAME
        except Exception:
            srv = os.getenv("APP_NAME", "HighPerfStatelessService")

    clean_srv = str(srv).strip() if srv else ""
    clean_res = str(res or "").strip().lower()
    clean_act = str(act or "").strip().lower()

    parts: list[str] = []
    if clean_srv:
        parts.append(clean_srv)
    if clean_res:
        parts.append(clean_res)
    if clean_act:
        parts.append(clean_act)

    if ver is not None:
        clean_ver = str(ver).strip().lower()
        if clean_ver.isdigit():
            clean_ver = f"v{clean_ver}"
        if clean_ver:
            parts.append(clean_ver)

    return ".".join(parts)


class BaseService(IBaseService[ModelType, CreateSchemaType, UpdateSchemaType, ReadSchemaType, IdType]):
    """Generic domain service orchestrator implementing IBaseService with polymorphic caching and Elasticsearch search offloading."""

    def __init__(
        self,
        repo: IBaseRepository[ModelType, IdType],
        read_schema_cls: Type[ReadSchemaType],
        cache: Optional[ICacheService] = None,
        elasticsearch: Optional[IElasticsearchService] = None,
        order_col: str = "created_at",
        id_col: str = "id",
        db: Optional[AsyncSession] = None,
        session: Optional[AsyncSession] = None,
        outbox_enabled: bool = False,
        auto_outbox_events: bool = False,
        outbox_topic: str = "domain.events",
        service_name: Optional[str] = None,
    ) -> None:
        self.repo: IBaseRepository[ModelType, IdType] = repo
        self.repository: IBaseRepository[ModelType, IdType] = repo
        active_db = db or session or getattr(repo, "_session", None) or getattr(repo, "session", None) or getattr(repo, "db", None)
        self.db: Optional[AsyncSession] = active_db
        self.session: Optional[AsyncSession] = active_db
        self.read_schema_cls: Type[ReadSchemaType] = read_schema_cls
        # Polymorphic caching: uses NullCache when no distributed cache is supplied
        self.cache: ICacheService = cache if cache is not None else NullCache()
        # Elasticsearch CQRS search engine: uses NullElasticsearch when disabled/absent
        self.elasticsearch: IElasticsearchService = (
            elasticsearch if elasticsearch is not None else NullElasticsearch()
        )
        self.es: IElasticsearchService = self.elasticsearch
        self.order_col: str = order_col
        self.id_col: str = id_col
        self.outbox_enabled = outbox_enabled
        self.auto_outbox_events = auto_outbox_events
        self.outbox_topic = outbox_topic
        self.service_name = service_name or get_settings().APP_NAME

    @property
    def resource_name(self) -> str:
        """Returns the domain entity resource name."""
        return self.repo.model.__name__

    # --------------------------------------------------------------------------
    # Pluggable Domain Lifecycle Hooks
    # --------------------------------------------------------------------------

    async def before_create(self, payload: CreateSchemaType) -> ModelType:
        """Hook called before inserting entity into repository.

        Default implementation instantiates model directly from payload dictionary.
        Override to implement custom ID generation, encryption, or blind indexing.
        """
        data: Dict[str, Any] = payload.model_dump()
        entity: ModelType = self.repo.model(**data)
        id_column = self.repo.model.__table__.c.get(self.id_col)
        if (
            id_column is not None
            and isinstance(id_column.type, Uuid)
            and getattr(entity, self.id_col, None) is None
        ):
            setattr(entity, self.id_col, uuid.uuid4())
        if hasattr(entity, "created_at") and getattr(entity, "created_at", None) is None:
            setattr(entity, "created_at", datetime.now(timezone.utc))
        return entity

    async def before_update(self, entity: ModelType, payload: UpdateSchemaType) -> None:
        """Hook called before updating entity.

        Uses model_dump(exclude_unset=True) to support partial updates safely.
        """
        update_data: Dict[str, Any] = payload.model_dump(exclude_unset=True)
        for key, val in update_data.items():
            if hasattr(entity, key) and val is not None:
                setattr(entity, key, val)

    async def before_delete(self, entity: ModelType) -> None:
        """Hook called before deleting entity. Override to enforce custom cascading rules."""
        pass

    async def after_create(self, entity: ModelType) -> None:
        """Hook called after the entity has been created and the transaction committed.

        Override for side effects that require the entity to already be durably
        persisted (e.g. sending a welcome email, scheduling a follow-up job, audit
        logging). Runs after `before_create`/repository insert/commit, but before
        the Elasticsearch CQRS sync.
        """
        pass

    async def after_update(self, entity: ModelType, payload: UpdateSchemaType) -> None:
        """Hook called after the entity has been updated and the transaction committed.

        Override for post-update side effects (e.g. notifying subscribers of the
        change). `payload` is the original update DTO, in case the hook needs to
        know which fields were explicitly set (`payload.model_dump(exclude_unset=True)`).
        """
        pass

    async def after_delete(self, entity: ModelType) -> None:
        """Hook called after the entity has been deleted and the transaction committed.

        Override for cleanup side effects (e.g. removing dependent cached resources,
        emitting a deletion notification). The `entity` instance reflects its last
        in-memory state prior to deletion.
        """
        pass

    async def to_read_schema(self, entity: ModelType) -> ReadSchemaType:
        """Hook converting database model to public read schema.

        Override to decrypt ciphertext or mask PII fields before returning.
        """
        if hasattr(entity, "created_at") and getattr(entity, "created_at", None) is None:
            setattr(entity, "created_at", datetime.now(timezone.utc))
        return self.read_schema_cls.model_validate(entity)

    async def to_cache_dict(self, entity: ModelType) -> Dict[str, Any]:
        """Hook serializing database model to safe cached representation.

        Defaults to the JSON representation of the read schema.
        """
        read_model: ReadSchemaType = await self.to_read_schema(entity)
        return read_model.model_dump(mode="json")

    def build_event_type(
        self,
        action: str,
        resource: Optional[str] = None,
        model: Optional[str] = None,
        service: Optional[str] = None,
        version: Optional[str] = None,
    ) -> str:
        """Construct the standardized event_type following the '<service_name>.<model_name>.<action_name>[.<version>]' convention.

        Pulls the service name from `self.service_name` (defaults to settings.APP_NAME)
        and the model name from `self.resource_name.lower()`.
        """
        mod = model or resource or self.resource_name
        srv = service or getattr(self, "service_name", None)
        return format_event_type(model=mod, action=action, service=srv, version=version)

    def enqueue_outbox_event(
        self,
        action: str,
        entity: ModelType,
        *,
        event_type: Optional[str] = None,
        model: Optional[str] = None,
        resource: Optional[str] = None,
        service: Optional[str] = None,
        version: Optional[str] = None,
        extra_data: Optional[Dict[str, Any]] = None,
        force: bool = False,
    ) -> Optional[OutboxEvent]:
        """Explicitly enqueue an outbox event.

        Allows developers to emit domain events on demand when needed,
        without relying on automatic dynamic triggers from generic CRUD operations.

        Convention: '<service_name>.<model_name>.<action_name>[.<version>]'
        (e.g., 'HighPerfStatelessService.user.created').
        """
        if not self.outbox_enabled and not force:
            return None
        return self._enqueue_outbox_event(
            action=action,
            entity=entity,
            event_type=event_type,
            model=model or resource,
            service=service,
            version=version,
            extra_data=extra_data,
        )

    def _enqueue_outbox_event(
        self,
        action: str,
        entity: ModelType,
        event_type: Optional[str] = None,
        extra_data: Optional[Dict[str, Any]] = None,
        model: Optional[str] = None,
        resource: Optional[str] = None,
        service: Optional[str] = None,
        version: Optional[str] = None,
    ) -> Optional[OutboxEvent]:
        """Adds a non-sensitive event envelope to the current DB transaction.

        Uses `build_event_type` to guarantee the standardized event_type convention:
        '<service_name>.<model_name>.<action_name>[.<version>]' (e.g., 'HighPerfStatelessService.user.created').
        """
        if not self.outbox_enabled:
            return None
        if self.db is None:
            raise RuntimeError("Kafka outbox events require an active database session.")

        entity_id = str(getattr(entity, self.id_col))
        event_id = uuid.uuid4()
        resolved_event_type = event_type or self.build_event_type(
            action=action,
            model=model or resource,
            service=service,
            version=version,
        )
        outbox_event = OutboxEvent(
            id=event_id,
            topic=self.outbox_topic,
            event_key=entity_id,
            event_type=resolved_event_type,
            payload={
                "event_id": str(event_id),
                "event_type": resolved_event_type,
                "aggregate_type": self.resource_name,
                "aggregate_id": entity_id,
                "occurred_at": datetime.now(timezone.utc).isoformat(),
                "data": {"id": entity_id, **(extra_data or {})},
            },
        )
        self.db.add(outbox_event)
        return outbox_event

    # --------------------------------------------------------------------------
    # Generic CRUD Operations
    # --------------------------------------------------------------------------

    async def create(
        self,
        payload: CreateSchemaType,
        cache_enabled: Optional[bool] = None,
        emit_outbox_event: Optional[bool] = None,
    ) -> ReadSchemaType:
        """Creates entity, invalidates list cache, commits transaction, syncs to ES, and returns read schema."""
        entity: ModelType = await self.before_create(payload)
        try:
            await self.repo.create(entity)
            if self.outbox_enabled and (
                emit_outbox_event is True
                or (emit_outbox_event is None and self.auto_outbox_events is True)
            ):
                self._enqueue_outbox_event("created", entity)
            await self.cache.invalidate_lists(enabled=cache_enabled)
            if self.db is not None:
                await self.db.commit()

            await self.after_create(entity)

            # CQRS: sync created document to Elasticsearch for text search offloading
            try:
                doc = await self.to_cache_dict(entity)
                entity_id = getattr(entity, self.id_col, "")
                await self.elasticsearch.index_document(
                    index=self.resource_name.lower(),
                    id_=str(entity_id),
                    document=doc,
                )
            except Exception as es_err:
                logger.warning("Elasticsearch index failed on create (%s): %s", self.resource_name, es_err)

            return await self.to_read_schema(entity)
        except IntegrityError as exc:
            if self.db is not None:
                await self.db.rollback()
            raise ConflictError("Conflict: duplicate unique constraint violation.") from exc

    async def get(
        self,
        entity_id: IdType,
        cache_enabled: Optional[bool] = None,
    ) -> Optional[ReadSchemaType]:
        """Retrieves entity by ID returning None if not found."""
        try:
            return await self.get_by_id(entity_id, cache_enabled=cache_enabled)
        except NotFoundError:
            return None

    async def get_by_id(
        self,
        entity_id: IdType,
        cache_enabled: Optional[bool] = None,
    ) -> ReadSchemaType:
        """Retrieves entity by ID using polymorphic cache-aside with stampede protection."""
        # 1. Check item cache (NullCache immediately returns None)
        cached_data: Optional[Dict[str, Any]] = await self.cache.get_item(entity_id, enabled=cache_enabled)
        if cached_data is not None:
            if cached_data == NEGATIVE_CACHE_SENTINEL:
                raise NotFoundError(f"{self.repo.model.__name__} with ID '{entity_id}' not found")
            return self.read_schema_cls(**cached_data)

        # 2. Cache stampede-protected fetch on cache miss (NullCache executes _fetch directly)
        async def _fetch() -> Optional[Dict[str, Any]]:
            entity: Optional[ModelType] = await self.repo.get(entity_id)
            if entity is None:
                return None
            return await self.to_cache_dict(entity)

        data: Optional[Dict[str, Any]] = await self.cache.get_or_compute_item(entity_id, _fetch, enabled=cache_enabled)
        if data is None:
            raise NotFoundError(f"{self.repo.model.__name__} with ID '{entity_id}' not found")

        return self.read_schema_cls(**data)

    async def get_all(
        self,
        limit: int = 100,
        offset: int = 0,
        **filters: Any,
    ) -> List[ReadSchemaType]:
        """Retrieves all entities matching filters with limit and offset."""
        if hasattr(self.repo, "get_all"):
            entities = await self.repo.get_all(limit=limit, offset=offset, **filters)
            return [await self.to_read_schema(e) for e in entities]
        return await self.list_offset(offset=offset, limit=limit, **filters)

    async def update(
        self,
        entity_id: IdType,
        payload: UpdateSchemaType,
        cache_enabled: Optional[bool] = None,
        emit_outbox_event: Optional[bool] = None,
    ) -> Optional[ReadSchemaType]:
        """Updates entity, commits transaction, and returns updated read schema or None if not found."""
        entity: Optional[ModelType] = await self.repo.get(entity_id)
        if entity is None:
            return None

        try:
            await self.before_update(entity, payload)
            if hasattr(self.repo, "update"):
                await self.repo.update(entity)
            else:
                await self.repo.flush()
            if self.outbox_enabled and (
                emit_outbox_event is True
                or (emit_outbox_event is None and self.auto_outbox_events is True)
            ):
                self._enqueue_outbox_event("updated", entity)

            # Invalidate item and list caches polymorphically
            await self.cache.delete_item(entity_id, enabled=cache_enabled)
            await self.cache.invalidate_lists(enabled=cache_enabled)

            if self.db is not None:
                await self.db.commit()

            await self.after_update(entity, payload)

            # CQRS: update document in Elasticsearch
            try:
                doc = await self.to_cache_dict(entity)
                await self.elasticsearch.update_document(
                    index=self.resource_name.lower(),
                    id_=str(entity_id),
                    document=doc,
                )
            except Exception as es_err:
                logger.warning("Elasticsearch index failed on update (%s): %s", self.resource_name, es_err)

            return await self.to_read_schema(entity)
        except IntegrityError as exc:
            if self.db is not None:
                await self.db.rollback()
            raise ConflictError("Conflict: update violates unique database constraint.") from exc

    async def update_by_id(
        self,
        entity_id: IdType,
        payload: UpdateSchemaType,
        cache_enabled: Optional[bool] = None,
        emit_outbox_event: Optional[bool] = None,
    ) -> ReadSchemaType:
        """Updates entity by ID, raising NotFoundError if entity does not exist."""
        updated = await self.update(
            entity_id,
            payload,
            cache_enabled=cache_enabled,
            emit_outbox_event=emit_outbox_event,
        )
        if updated is None:
            raise NotFoundError(f"{self.repo.model.__name__} with ID '{entity_id}' not found")
        return updated

    async def delete(
        self,
        entity_id: IdType,
        cache_enabled: Optional[bool] = None,
        emit_outbox_event: Optional[bool] = None,
    ) -> bool:
        """Deletes entity and invalidates item and list caches. Returns True if deleted, False if not found."""
        entity: Optional[ModelType] = await self.repo.get(entity_id)
        if entity is None:
            return False

        await self.before_delete(entity)
        await self.repo.delete(entity)
        if self.outbox_enabled and (
            emit_outbox_event is True
            or (emit_outbox_event is None and self.auto_outbox_events is True)
        ):
            self._enqueue_outbox_event("deleted", entity)

        # Invalidate item and list caches polymorphically
        await self.cache.delete_item(entity_id, enabled=cache_enabled)
        await self.cache.invalidate_lists(enabled=cache_enabled)

        if self.db is not None:
            await self.db.commit()

        await self.after_delete(entity)

        # CQRS: remove document from Elasticsearch
        try:
            await self.elasticsearch.delete_document(
                index=self.resource_name.lower(),
                id_=str(entity_id),
            )
        except Exception as es_err:
            logger.warning("Elasticsearch delete failed on delete (%s): %s", self.resource_name, es_err)

        return True

    async def delete_by_id(
        self,
        entity_id: IdType,
        cache_enabled: Optional[bool] = None,
        emit_outbox_event: Optional[bool] = None,
    ) -> None:
        """Deletes entity by ID, raising NotFoundError if entity not found."""
        deleted = await self.delete(
            entity_id,
            cache_enabled=cache_enabled,
            emit_outbox_event=emit_outbox_event,
        )
        if not deleted:
            raise NotFoundError(f"{self.repo.model.__name__} with ID '{entity_id}' not found")

    async def list_keyset(
        self,
        limit: int = 50,
        cursor: Optional[str] = None,
        cache_enabled: Optional[bool] = None,
        **filters: Any,
    ) -> Dict[str, Any]:
        """Dynamic Keyset (cursor) pagination with polymorphic namespace-versioned list caching."""
        safe_limit: int = min(100, max(1, limit))
        filter_params: Dict[str, Any] = {"limit": safe_limit, "cursor": cursor, **filters}

        # 1. Check list cache (NullCache immediately returns None)
        cached_page: Optional[Dict[str, Any]] = await self.cache.get_paginated(enabled=cache_enabled, **filter_params)
        if cached_page is not None:
            return cached_page

        # 2. Decode keyset cursor
        cursor_dt: Optional[datetime] = None
        cursor_id: Optional[Any] = None
        if cursor:
            cursor_dt, cursor_id = decode_cursor(cursor)

        # 3. Query repository dynamically
        items_db: List[ModelType]
        has_next: bool
        items_db, has_next = await self.repo.list_keyset(
            limit=safe_limit,
            cursor_dt=cursor_dt,
            cursor_id=cursor_id,
            order_col=self.order_col,
            id_col=self.id_col,
            **filters,
        )

        # 4. Transform entities using domain hook
        items_schema: List[ReadSchemaType] = [await self.to_read_schema(u) for u in items_db]

        # 5. Build next cursor if additional page exists
        next_cursor: Optional[str] = None
        if has_next and items_db:
            last_item: ModelType = items_db[-1]
            last_dt: Any = getattr(last_item, self.order_col)
            last_id: Any = getattr(last_item, self.id_col)
            next_cursor = encode_cursor(last_dt, last_id)

        page_result: Dict[str, Any] = {
            "items": [item.model_dump(mode="json") for item in items_schema],
            "next_cursor": next_cursor,
            "limit": safe_limit,
        }

        # 6. Cache result polymorphically
        await self.cache.set_paginated(page_result, enabled=cache_enabled, **filter_params)

        return page_result

    async def list_offset(
        self,
        offset: int = 0,
        limit: int = 50,
        order_by: Optional[str] = None,
        descending: bool = False,
        **filters: Any,
    ) -> List[ReadSchemaType]:
        """Standard offset-based listing for smaller datasets or admin screens."""
        safe_limit: int = min(100, max(1, limit))
        entities: Sequence[ModelType] = await self.repo.list(
            offset=offset,
            limit=safe_limit,
            order_by=order_by,
            descending=descending,
            **filters,
        )
        return [await self.to_read_schema(e) for e in entities]

    async def list(
        self,
        *,
        filters: Optional[Mapping[str, Any]] = None,
        sort: Optional[Sequence[str]] = None,
        page: int = 1,
        size: int = 20,
    ) -> Tuple[Sequence[ReadSchemaType], int]:
        """Paginated listing returning (items, total) to power CRUDRouter and Page schemas."""
        safe_size: int = min(100, max(1, size))
        safe_page: int = max(1, page)
        total = await self.repo.count(filters=filters)
        entities = await self.repo.list(
            filters=filters,
            sort=sort,
            offset=(safe_page - 1) * safe_size,
            limit=safe_size,
        )
        items = [await self.to_read_schema(e) for e in entities]
        return items, total

    async def search(
        self,
        query: Union[str, Dict[str, Any]],
        limit: int = 20,
        offset: int = 0,
    ) -> Tuple[List[Dict[str, Any]], int]:
        """Full-text search offloaded directly to Elasticsearch (CQRS)."""
        return await self.elasticsearch.search(
            index=self.resource_name.lower(),
            query=query,
            limit=limit,
            offset=offset,
        )


__all__ = [
    "BaseService",
    "format_event_type",
]
