"""User Service: Clean domain orchestration with direct Redis caching, Transactional Outbox, and Elasticsearch search."""

import asyncio
from datetime import datetime, timezone
import logging
from typing import Annotated, Any, Dict, List, Optional, Union
import uuid

import asyncpg
from fastapi import Depends, Request
import orjson

from app.api.deps import CryptoDep, ElasticsearchDep, RedisDep, SessionDep
from app.core.cache_interface import ICacheService, NullCache
from app.core.config import Settings, get_settings
from app.core.elasticsearch import IElasticsearchService, NullElasticsearch
from app.core.exceptions import ConflictError, DecryptionError, NotFoundError
from app.core.security import CryptoService
from app.db.pool import UnitOfWork
from app.models.outbox import OutboxEvent
from app.models.user import User, UserAccount, UserProfile
from app.repositories.outbox import OutboxRepository
from app.repositories.user import UserRepository
from app.schemas.base import decode_cursor, encode_cursor
from app.schemas.user import (
    AccountRead,
    DepartmentRead,
    RoleRead,
    SearchAggregations,
    UserCreate,
    UserPage,
    UserRead,
    UserSearchQuery,
    UserSearchResult,
    UserUpdate,
    UserProfileRead,
)
from app.services.cache import DynamicRedisCache

logger = logging.getLogger(__name__)


def _parse_datetime(val: Any) -> Optional[datetime]:
    """Safely parses string or datetime value into Optional[datetime]."""
    if isinstance(val, datetime):
        return val
    if isinstance(val, str) and val.strip():
        try:
            return datetime.fromisoformat(val.strip())
        except ValueError:
            return None
    return None


class UserService:
    """User domain service: PII cryptography, Redis L1 caching, Transactional Outbox, and Elasticsearch search."""

    def __init__(
        self,
        db: UnitOfWork,
        cache: Optional[ICacheService] = None,
        crypto: Optional[CryptoService] = None,
        elasticsearch: Optional[IElasticsearchService] = None,
        outbox_worker: Optional[Any] = None,
        **_: Any,
    ) -> None:
        self.db = db
        self.repo = UserRepository(db)
        self.outbox_repo = OutboxRepository(db)
        self.cache: ICacheService = cache or NullCache()
        self.crypto: Optional[CryptoService] = crypto
        self.es: IElasticsearchService = elasticsearch or NullElasticsearch()
        self.outbox_worker = outbox_worker

    
    # --------------------------------------------------------------------------
    # 1. CREATE USER (POST /users)
    # --------------------------------------------------------------------------
    async def create_user(self, payload: UserCreate) -> UserRead:
        """Create a user with PII encryption, HMAC blind index, Redis pre-warming, and Outbox ES sync."""
        # Uniqueness check for email
        existing_email = await self.repo.get_by_email(str(payload.email))
        if existing_email:
            raise ConflictError("A user with this email address already exists.")

        user_id = uuid.uuid4()
        phone_encrypted = payload.phone
        phone_blind_index = payload.phone

        # Cryptographic phone encryption & HMAC blind index
        if self.crypto is not None:
            blind_index = self.crypto.blind_index(payload.phone)
            existing_phone = await self.repo.get_by_phone_blind_index(blind_index)
            if existing_phone:
                raise ConflictError("A user with this phone number already exists.")

            aad = f"users.phone:{user_id}".encode("utf-8")
            phone_encrypted = self.crypto.encrypt(payload.phone, aad=aad)
            phone_blind_index = blind_index

        user = User(
            id=user_id,
            name=payload.name,
            email=str(payload.email),
            role=payload.role,
            department=payload.department,
            department_id=payload.department_id,
            role_id=payload.role_id,
            phone_encrypted=phone_encrypted,
            phone_blind_index=phone_blind_index,
            created_at=datetime.now(timezone.utc),
        )

        # Validate accounts uniqueness upfront
        if payload.accounts:
            acc_numbers = [acc.account_number for acc in payload.accounts]
            existing_accounts = await self.repo.get_existing_account_numbers(acc_numbers)
            if existing_accounts:
                raise ConflictError(f"Account number '{existing_accounts[0]}' already exists.")

        try:
            created = await self.repo.create(user)

            # Optional initial accounts creation in a single batch (no loop)
            accounts_read: Optional[List[AccountRead]] = None
            total_balance_cents: Optional[int] = None
            account_count: Optional[int] = None
            if payload.accounts:
                now = datetime.now(timezone.utc)
                accounts = [
                    UserAccount(
                        id=uuid.uuid4(),
                        user_id=user_id,
                        account_number=acc.account_number,
                        account_type=acc.account_type,
                        balance_cents=acc.balance_cents,
                        currency=acc.currency,
                        status=acc.status,
                        created_at=now,
                        updated_at=now,
                    )
                    for acc in payload.accounts
                ]
                await self.repo.create_accounts(accounts)
                accounts_read = [
                    AccountRead(
                        id=acc.id,
                        account_number=acc.account_number,
                        account_type=acc.account_type,
                        balance_cents=acc.balance_cents,
                        currency=acc.currency,
                        status=acc.status,
                        created_at=acc.created_at,
                    )
                    for acc in accounts
                ]
                account_count = len(accounts_read)
                total_balance_cents = sum(acc.balance_cents for acc in accounts_read)
        except asyncpg.exceptions.UniqueViolationError as exc:
            raise ConflictError(f"Unique constraint violation: {exc.detail or exc}")

        read_model = self._to_read_schema(
            created,
            accounts=accounts_read,
            total_balance_cents=total_balance_cents,
            account_count=account_count,
        )

        # Stage Outbox Event BEFORE DB commit (Syncs both User and UserProfile to Elasticsearch)
        es_payload = await self._build_user_es_payload(created.id)
        outbox_event = await self.outbox_repo.create_event(
            aggregate_type="user",
            aggregate_id=str(created.id),
            event_type="CREATE",
            payload=es_payload,
        )

        # Cache synchronization:
        #   - Invalidate list cache (O(1) version bump) to prevent stale paginated list reads.
        #   - Pre-warm item cache (write-through) for instant sub-millisecond subsequent reads.
        await self.cache.invalidate_lists()
        await self.cache.set_item(str(created.id), read_model.model_dump(mode="json"))
        await self.cache.delete_item(f"profile:{created.id}")

        # Atomic commit
        await self.db.commit()

        # Dispatch Outbox synchronization to Elasticsearch
        await self._dispatch_outbox_sync(outbox_event)

        return read_model

    # --------------------------------------------------------------------------
    # 2. GET USER BY ID (GET /users/{user_id})
    # --------------------------------------------------------------------------
    async def get_user_by_id(self, user_id: uuid.UUID) -> UserRead:
        """Fetch user by ID with 3-tier Self-Healing Fallback (Redis -> ES -> PostgreSQL)."""
        # Level 1: Redis L1 Cache-Aside
        try:
            cached = await self.cache.get_item(str(user_id))
            if cached is not None:
                if cached.get("__not_found__"):
                    raise NotFoundError(f"User with ID '{user_id}' not found.")
                return UserRead(**cached)
        except NotFoundError:
            raise
        except Exception as exc:
            logger.warning("Redis read error in get_user_by_id: %s", exc)

        # Level 2: Elasticsearch L2
        try:
            es_doc = await self.es.get_document(index="user", id_=str(user_id))
            if es_doc is not None:
                user_read = UserRead(
                    id=es_doc["id"],
                    name=es_doc.get("name", ""),
                    email=es_doc.get("email", ""),
                    role=es_doc.get("role", "user"),
                    department=es_doc.get("department"),
                    department_id=es_doc.get("department_id") or es_doc.get("department"),
                    role_id=es_doc.get("role_id") or es_doc.get("role"),
                    phone_masked=es_doc.get("phone_masked"),
                    created_at=_parse_datetime(es_doc.get("created_at")),
                )
                try:
                    await self.cache.set_item(str(user_id), user_read.model_dump(mode="json"))
                except Exception:
                    pass
                return user_read
        except Exception as exc:
            logger.warning("Elasticsearch read error in get_user_by_id: %s", exc)

        # Level 3: Fallback to PostgreSQL (Authoritative Store)
        user = await self.repo.get(user_id)
        if user is None:
            try:
                await self.cache.set_negative_cache(str(user_id))
            except Exception:
                pass
            raise NotFoundError(f"User with ID '{user_id}' not found.")

        read_model = self._to_read_schema(user)

        # Self-Heal: Backfill both Redis and Elasticsearch
        try:
            await self.cache.set_item(str(user.id), read_model.model_dump(mode="json"))
        except Exception as exc:
            logger.warning("Failed to backfill Redis: %s", exc)

        try:
            await self.es.index_document(index="user", id_=str(user_id), document=self._build_user_es_doc(user))
        except Exception as exc:
            logger.warning("Failed to backfill Elasticsearch: %s", exc)

        return read_model

    # --------------------------------------------------------------------------
    # 2b. GET USER PROFILE (GET /users/{user_id}/profile)
    # --------------------------------------------------------------------------
    async def get_user_profile(self, user_id: uuid.UUID) -> UserProfileRead:
        """Fetch rich user profile with 3-tier Self-Healing Fallback (Redis -> ES -> PostgreSQL)."""
        cache_key = f"profile:{user_id}"

        # Level 1: Redis L1 Cache-Aside
        try:
            cached = await self.cache.get_item(cache_key)
            if cached is not None:
                if isinstance(cached, dict) and cached.get("__not_found__"):
                    raise NotFoundError(f"User with ID '{user_id}' not found.")
                return UserProfileRead(**cached)
        except NotFoundError:
            raise
        except Exception as exc:
            logger.warning("Redis read error in get_user_profile: %s", exc)

        # Level 2: Elasticsearch L2
        try:
            es_doc = await self.es.get_document(index="user_profile", id_=str(user_id))
            if es_doc is not None:
                profile_read = UserProfileRead(**es_doc)
                try:
                    await self.cache.set_item(cache_key, profile_read.model_dump(mode="json"))
                except Exception:
                    pass
                return profile_read
        except Exception as exc:
            logger.warning("Elasticsearch read error in get_user_profile: %s", exc)

        # Level 3: Fallback to PostgreSQL (Authoritative Store)
        profile = await self.repo.get_profile(user_id)
        if profile is None:
            try:
                await self.cache.set_negative_cache(cache_key)
            except Exception:
                pass
            raise NotFoundError(f"User with ID '{user_id}' not found.")

        profile_read = self._to_profile_read(profile)

        # Self-Heal: Backfill both Redis and Elasticsearch
        try:
            await self.cache.set_item(cache_key, profile_read.model_dump(mode="json"))
        except Exception as exc:
            logger.warning("Failed to backfill Redis profile: %s", exc)

        try:
            profile_doc = profile_read.model_dump(mode="json")
            profile_doc["department_name"] = profile_read.department.name if profile_read.department else None
            profile_doc["department_code"] = profile_read.department.code if profile_read.department else None
            profile_doc["role_name"] = profile_read.role_info.name if profile_read.role_info else None
            profile_doc["account_numbers"] = [a.account_number for a in profile_read.accounts]
            await self.es.index_document(index="user_profile", id_=str(user_id), document=profile_doc)
        except Exception as exc:
            logger.warning("Failed to backfill Elasticsearch profile: %s", exc)

        return profile_read

    # --------------------------------------------------------------------------
    # 3. LIST USERS (GET /users - Keyset Cursor Pagination)
    # --------------------------------------------------------------------------
    async def list_users(
        self,
        limit: int = 50,
        cursor: Optional[str] = None,
        role: Optional[str] = None,
    ) -> UserPage:
        """List users using keyset (cursor) B-Tree pagination and Redis list caching."""
        safe_limit = min(100, max(1, limit))

        cached_page = await self.cache.get_paginated(
            limit=safe_limit,
            cursor=cursor,
            role=role,
        )
        if cached_page is not None:
            return UserPage(**cached_page)

        cursor_dt: Optional[datetime] = None
        cursor_id: Optional[Any] = None
        if cursor:
            cursor_dt, cursor_id = decode_cursor(cursor)

        users_db, has_next = await self.repo.list_keyset(
            limit=safe_limit,
            cursor_dt=cursor_dt,
            cursor_id=cursor_id,
            role=role,
        )

        items_schema = [self._to_read_schema(u) for u in users_db]

        next_cursor: Optional[str] = None
        if has_next and users_db and users_db[-1].created_at:
            next_cursor = encode_cursor(users_db[-1].created_at, users_db[-1].id)

        user_page = UserPage(items=items_schema, next_cursor=next_cursor)
        await self.cache.set_paginated(
            user_page.model_dump(mode="json"),
            limit=safe_limit,
            cursor=cursor,
            role=role,
        )
        return user_page

    # --------------------------------------------------------------------------
    # 4. UPDATE USER (PATCH /users/{user_id})
    # --------------------------------------------------------------------------
    async def update_user(self, user_id: uuid.UUID, payload: UserUpdate) -> UserRead:
        """Partially update user fields with selective re-encryption, cache invalidation, and Outbox sync."""
        user = await self.repo.get(user_id)
        if user is None:
            raise NotFoundError(f"User with ID '{user_id}' not found.")

        updates = payload.model_dump(exclude_unset=True)

        if "email" in updates and updates["email"] is not None:
            existing = await self.repo.get_by_email(str(updates["email"]))
            if existing and str(existing.id) != str(user.id):
                raise ConflictError("A user with this email address already exists.")
            user.email = str(updates["email"])

        if payload.name is not None:
            user.name = payload.name
        if payload.role is not None:
            user.role = payload.role
        if payload.department is not None:
            user.department = payload.department
        if payload.department_id is not None:
            user.department_id = payload.department_id
        if payload.role_id is not None:
            user.role_id = payload.role_id

        if payload.phone is not None:
            if self.crypto is not None:
                aad = f"users.phone:{user.id}".encode("utf-8")
                user.phone_encrypted = self.crypto.encrypt(payload.phone, aad=aad)
                user.phone_blind_index = self.crypto.blind_index(payload.phone)
            else:
                user.phone_encrypted = payload.phone
                user.phone_blind_index = payload.phone

        updated = await self.repo.update(user)
        read_model = self._to_read_schema(updated)

        # Stage Outbox Event BEFORE DB commit (Syncs both User and UserProfile to Elasticsearch)
        es_payload = await self._build_user_es_payload(updated.id)
        outbox_event = await self.outbox_repo.create_event(
            aggregate_type="user",
            aggregate_id=str(updated.id),
            event_type="UPDATE",
            payload=es_payload,
        )

        # Invalidate Redis item & list cache
        await self.cache.delete_item(str(user_id))
        await self.cache.delete_item(f"profile:{user_id}")
        await self.cache.invalidate_lists()

        # Atomic commit
        await self.db.commit()

        # Dispatch Outbox sync to Elasticsearch
        await self._dispatch_outbox_sync(outbox_event)

        return read_model

    # --------------------------------------------------------------------------
    # 5. DELETE USER (DELETE /users/{user_id})
    # --------------------------------------------------------------------------
    async def delete_user(self, user_id: uuid.UUID) -> None:
        """Delete user, purge Redis cache, and record DELETE event in Transactional Outbox."""
        outbox_event = await self.outbox_repo.create_event(
            aggregate_type="user",
            aggregate_id=str(user_id),
            event_type="DELETE",
            payload={},
        )

        deleted = await self.repo.delete(user_id)
        if not deleted:
            raise NotFoundError(f"User with ID '{user_id}' not found.")

        # Invalidate Redis
        await self.cache.delete_item(str(user_id))
        await self.cache.delete_item(f"profile:{user_id}")
        await self.cache.invalidate_lists()

        # Atomic commit
        await self.db.commit()

        # Dispatch Outbox sync (Deletes from ES and marks outbox published)
        await self._dispatch_outbox_sync(outbox_event)

    # --------------------------------------------------------------------------
    # 5b. BULK SEED DIVERSE USERS (POST /users/seed)
    # --------------------------------------------------------------------------
    async def seed_diverse_users(self, count: int = 10) -> int:
        """Seeds `count` diverse users into PostgreSQL using raw SQL without loops."""
        safe_count = min(5000, max(1, count))
        await self.repo.seed_users(safe_count)
        await self.cache.invalidate_lists()
        await self.db.commit()
        return safe_count

    # --------------------------------------------------------------------------
    # 6. COMPLEX SEARCH USERS (Elasticsearch CQRS with PostgreSQL Fallback)
    # --------------------------------------------------------------------------
    async def search_users(
        self,
        query: Optional[Union[UserSearchQuery, str]] = None,
        *,
        q: Optional[str] = None,
        department: Optional[str] = None,
        role: Optional[str] = None,
        account_type: Optional[str] = None,
        currency: Optional[str] = None,
        min_balance_cents: Optional[int] = None,
        max_balance_cents: Optional[int] = None,
        min_accounts: Optional[int] = None,
        max_accounts: Optional[int] = None,
        created_after: Optional[datetime] = None,
        created_before: Optional[datetime] = None,
        sort_by: Optional[str] = "created_at",
        sort_order: Optional[str] = "desc",
        limit: int = 20,
        offset: int = 0,
        include_aggregations: bool = False,
    ) -> UserSearchResult:
        """Complex search orchestrating multi-field full-text, faceted filters, numeric ranges, aggregations, and DB fallback."""
        if isinstance(query, UserSearchQuery):
            params = query
        else:
            params = UserSearchQuery(
                q=q or (query if isinstance(query, str) else None),
                department=department,
                role=role,
                account_type=account_type,
                currency=currency,
                min_balance_cents=min_balance_cents,
                max_balance_cents=max_balance_cents,
                min_accounts=min_accounts,
                max_accounts=max_accounts,
                created_after=created_after,
                created_before=created_before,
                sort_by=sort_by or "created_at",
                sort_order=sort_order or "desc",
                limit=limit,
                offset=offset,
                include_aggregations=include_aggregations,
            )

        # ----------------------------------------------------------------------
        # 1. Primary: Elasticsearch Complex Bool Query against `user_profile`
        # ----------------------------------------------------------------------
        try:
            must_clauses: List[Dict[str, Any]] = []
            filter_clauses: List[Dict[str, Any]] = []

            if params.q and params.q.strip():
                cleaned_q = params.q.strip()
                must_clauses.append({
                    "multi_match": {
                        "query": cleaned_q,
                        "fields": [
                            "name^3",
                            "email^2",
                            "department^1.5",
                            "department_name^1.5",
                            "department_code^1.5",
                            "role^1.5",
                            "role_name^1.5",
                            "account_numbers^2",
                        ],
                        "fuzziness": "AUTO",
                        "prefix_length": 2,
                    }
                })
            else:
                must_clauses.append({"match_all": {}})

            if params.department:
                dept_val = params.department.strip()
                filter_clauses.append({
                    "bool": {
                        "should": [
                            {"term": {"department.keyword": dept_val}},
                            {"term": {"department_code.keyword": dept_val}},
                            {"term": {"department_name.keyword": dept_val}},
                            {"match": {"department": dept_val}},
                            {"match": {"department_name": dept_val}},
                        ],
                        "minimum_should_match": 1,
                    }
                })

            if params.role:
                role_val = params.role.strip()
                filter_clauses.append({
                    "bool": {
                        "should": [
                            {"term": {"role.keyword": role_val}},
                            {"term": {"role_name.keyword": role_val}},
                            {"match": {"role": role_val}},
                            {"match": {"role_name": role_val}},
                        ],
                        "minimum_should_match": 1,
                    }
                })

            if params.account_type:
                filter_clauses.append({
                    "term": {"account_types.keyword": params.account_type.strip().lower()}
                })

            if params.currency:
                filter_clauses.append({
                    "term": {"currencies.keyword": params.currency.strip().upper()}
                })

            balance_range: Dict[str, Any] = {}
            if params.min_balance_cents is not None:
                balance_range["gte"] = params.min_balance_cents
            if params.max_balance_cents is not None:
                balance_range["lte"] = params.max_balance_cents
            if balance_range:
                filter_clauses.append({"range": {"total_balance_cents": balance_range}})

            accounts_range: Dict[str, Any] = {}
            if params.min_accounts is not None:
                accounts_range["gte"] = params.min_accounts
            if params.max_accounts is not None:
                accounts_range["lte"] = params.max_accounts
            if accounts_range:
                filter_clauses.append({"range": {"account_count": accounts_range}})

            date_range: Dict[str, Any] = {}
            if params.created_after is not None:
                date_range["gte"] = params.created_after.isoformat()
            if params.created_before is not None:
                date_range["lte"] = params.created_before.isoformat()
            if date_range:
                filter_clauses.append({"range": {"created_at": date_range}})

            es_query = {
                "bool": {
                    "must": must_clauses,
                    "filter": filter_clauses,
                }
            }

            sort_field = params.sort_by or "created_at"
            sort_order = (params.sort_order or "desc").lower()

            es_sort: List[Any] = []
            if sort_field in ("_score", "score", "relevance"):
                es_sort.append({"_score": {"order": sort_order}})
            elif sort_field in ("total_balance_cents", "account_count", "created_at"):
                es_sort.append({sort_field: {"order": sort_order}})
            elif sort_field in ("name", "email"):
                es_sort.append({f"{sort_field}.keyword": {"order": sort_order, "unmapped_type": "keyword"}})
            else:
                es_sort.append({"created_at": {"order": "desc"}})

            es_aggs: Dict[str, Any] = {}
            if params.include_aggregations:
                es_aggs = {
                    "departments": {"terms": {"field": "department_name.keyword", "size": 15}},
                    "roles": {"terms": {"field": "role_name.keyword", "size": 15}},
                    "account_types": {"terms": {"field": "account_types.keyword", "size": 15}},
                    "currencies": {"terms": {"field": "currencies.keyword", "size": 15}},
                    "balance_stats": {"stats": {"field": "total_balance_cents"}},
                }

            search_body: Dict[str, Any] = {
                "query": es_query,
                "sort": es_sort,
                "from": params.offset,
                "size": params.limit,
            }
            if es_aggs:
                search_body["aggs"] = es_aggs

            # First try against `user_profile` index (has accounts and balances)
            resp = await self.es.search_raw(index="user_profile", body=search_body)
            hits = resp.get("hits", {}).get("hits", [])
            total = int(resp.get("hits", {}).get("total", {}).get("value", 0))

            # If user_profile returned 0 results, check if basic `user` index has hits (e.g. users without accounts)
            if not hits and not (params.account_type or params.currency or params.min_balance_cents or params.max_balance_cents or params.min_accounts):
                resp = await self.es.search_raw(index="user", body=search_body)
                hits = resp.get("hits", {}).get("hits", [])
                total = int(resp.get("hits", {}).get("total", {}).get("value", 0))

            if hits or total > 0 or not self.db:
                parsed_items: List[UserProfileRead] = []
                for h in hits:
                    src = h.get("_source", {})
                    user_id = uuid.UUID(str(src["id"])) if isinstance(src.get("id"), (str, uuid.UUID)) else src.get("id")

                    dept_data = src.get("department")
                    if isinstance(dept_data, dict):
                        dept = DepartmentRead(**dept_data)
                    elif isinstance(dept_data, str) and dept_data.strip():
                        dept = DepartmentRead(id=0, name=dept_data, code=src.get("department_code") or dept_data, description="")
                    else:
                        dept = DepartmentRead(id=0, name="", code="", description="")

                    role_data = src.get("role_info")
                    if isinstance(role_data, dict):
                        role_info = RoleRead(**role_data)
                    elif isinstance(role_data, str) and role_data.strip():
                        role_info = RoleRead(id=0, name=role_data, description="", permissions=[])
                    else:
                        role_info = RoleRead(id=0, name=src.get("role", "user"), description="", permissions=[])

                    accs = [
                        AccountRead(
                            id=uuid.UUID(str(acc["id"])),
                            account_number=acc["account_number"],
                            account_type=acc.get("account_type", "checking"),
                            balance_cents=int(acc.get("balance_cents", 0)),
                            currency=acc.get("currency", "USD"),
                            status=acc.get("status", "active"),
                            created_at=_parse_datetime(acc.get("created_at")),
                        )
                        for acc in (src.get("accounts") or [])
                        if isinstance(acc, dict) and "id" in acc
                    ]

                    parsed_items.append(
                        UserProfileRead(
                            id=user_id,
                            name=src.get("name", ""),
                            email=src.get("email", ""),
                            role=src.get("role", "user"),
                            department=dept,
                            role_info=role_info,
                            accounts=accs,
                            total_balance_cents=int(src.get("total_balance_cents") or 0),
                            account_count=int(src.get("account_count") or len(accs)),
                            phone_masked=src.get("phone_masked"),
                            created_at=_parse_datetime(src.get("created_at")),
                        )
                    )

                parsed_aggs: Optional[SearchAggregations] = None
                if params.include_aggregations and "aggregations" in resp:
                    raw_aggs = resp["aggregations"]
                    dept_counts = {b["key"]: b["doc_count"] for b in raw_aggs.get("departments", {}).get("buckets", [])}
                    role_counts = {b["key"]: b["doc_count"] for b in raw_aggs.get("roles", {}).get("buckets", [])}
                    type_counts = {b["key"]: b["doc_count"] for b in raw_aggs.get("account_types", {}).get("buckets", [])}
                    curr_counts = {b["key"]: b["doc_count"] for b in raw_aggs.get("currencies", {}).get("buckets", [])}
                    bal_stats = raw_aggs.get("balance_stats", {})
                    parsed_aggs = SearchAggregations(
                        departments=dept_counts,
                        roles=role_counts,
                        account_types=type_counts,
                        currencies=curr_counts,
                        total_balance_cents={
                            "sum": bal_stats.get("sum", 0),
                            "avg": bal_stats.get("avg", 0),
                            "min": bal_stats.get("min", 0),
                            "max": bal_stats.get("max", 0),
                            "count": bal_stats.get("count", 0),
                        },
                    )

                return UserSearchResult(
                    total=total,
                    items=parsed_items,
                    limit=params.limit,
                    offset=params.offset,
                    aggregations=parsed_aggs,
                )
        except Exception as exc:
            logger.warning("Elasticsearch complex search failed: %s. Falling back to PostgreSQL.", exc)

        # ----------------------------------------------------------------------
        # 2. Secondary Fallback: PostgreSQL Complex Raw SQL Search
        # ----------------------------------------------------------------------
        db_rows, db_total = await self.repo.search_complex(
            q=params.q,
            department=params.department,
            role=params.role,
            account_type=params.account_type,
            currency=params.currency,
            min_balance_cents=params.min_balance_cents,
            max_balance_cents=params.max_balance_cents,
            min_accounts=params.min_accounts,
            max_accounts=params.max_accounts,
            created_after=params.created_after,
            created_before=params.created_before,
            sort_by=params.sort_by,
            sort_order=params.sort_order,
            limit=params.limit,
            offset=params.offset,
        )

        fallback_items: List[UserProfileRead] = []
        for r in db_rows:
            masked_phone = r.get("phone_encrypted")
            if self.crypto is not None and r.get("phone_encrypted"):
                aad = f"users.phone:{r['id']}".encode("utf-8")
                try:
                    plain = self.crypto.decrypt(r["phone_encrypted"], aad=aad)
                    masked_phone = self.crypto.mask_phone(plain)
                except Exception:
                    masked_phone = "***"

            dept_read = DepartmentRead(
                id=r.get("dept_id") or 0,
                name=r.get("dept_name") or r.get("department") or "",
                code=r.get("dept_code") or r.get("department") or "",
                description=r.get("dept_description") or "",
            )

            perms = r.get("role_permissions")
            role_read = RoleRead(
                id=r.get("role_id") or 0,
                name=r.get("role_name") or r.get("role") or "",
                description=r.get("role_description") or "",
                permissions=list(perms) if perms else [],
            )

            raw_accs = r.get("accounts_json") or []
            if isinstance(raw_accs, str):
                try:
                    raw_accs = orjson.loads(raw_accs)
                except Exception:
                    raw_accs = []

            accounts_list = [
                AccountRead(
                    id=uuid.UUID(str(acc["id"])),
                    account_number=acc["account_number"],
                    account_type=acc.get("account_type", "checking"),
                    balance_cents=int(acc.get("balance_cents", 0)),
                    currency=acc.get("currency", "USD"),
                    status=acc.get("status", "active"),
                    created_at=_parse_datetime(acc.get("created_at")),
                )
                for acc in raw_accs
                if isinstance(acc, dict) and "id" in acc
            ]

            fallback_items.append(
                UserProfileRead(
                    id=r["id"],
                    name=r["name"],
                    email=r["email"],
                    role=r["role"],
                    department=dept_read,
                    role_info=role_read,
                    accounts=accounts_list,
                    total_balance_cents=int(r.get("total_balance_cents") or 0),
                    account_count=int(r.get("account_count") or len(accounts_list)),
                    phone_masked=masked_phone,
                    created_at=r.get("created_at"),
                )
            )

        return UserSearchResult(
            total=db_total,
            items=fallback_items,
            limit=params.limit,
            offset=params.offset,
            aggregations=None,
        )

    

    # --------------------------------------------------------------------------
    # Helper Mappings & CQRS Synchronization
    # --------------------------------------------------------------------------
    def _to_read_schema(
        self,
        user: User,
        accounts: Optional[List[AccountRead]] = None,
        total_balance_cents: Optional[int] = None,
        account_count: Optional[int] = None,
    ) -> UserRead:
        """Decrypts phone using AAD and returns safe masked representation."""
        masked_phone = user.phone_encrypted
        if self.crypto is not None and user.phone_encrypted:
            aad = f"users.phone:{user.id}".encode("utf-8")
            try:
                plain_phone = self.crypto.decrypt(user.phone_encrypted, aad=aad)
                masked_phone = self.crypto.mask_phone(plain_phone)
            except DecryptionError:
                masked_phone = "***"

        return UserRead(
            id=user.id,
            name=user.name,
            email=user.email,
            role=user.role,
            department=user.department,
            department_id=user.department_id or user.department,
            role_id=user.role_id or user.role,
            phone_masked=masked_phone,
            accounts=accounts,
            total_balance_cents=total_balance_cents,
            account_count=account_count,
            created_at=user.created_at or datetime.now(timezone.utc),
        )

    def _to_profile_read(self, profile: UserProfile) -> UserProfileRead:
        """Converts UserProfile domain model to UserProfileRead schema with phone masking."""
        user = profile.user
        masked_phone = user.phone_encrypted
        if self.crypto is not None and user.phone_encrypted:
            aad = f"users.phone:{user.id}".encode("utf-8")
            try:
                plain_phone = self.crypto.decrypt(user.phone_encrypted, aad=aad)
                masked_phone = self.crypto.mask_phone(plain_phone)
            except DecryptionError:
                masked_phone = "***"

        dept_read = DepartmentRead(
            id=profile.department.id if profile.department else 0,
            name=profile.department.name if profile.department else (user.department or ""),
            code=profile.department.code if profile.department else (user.department or ""),
            description=profile.department.description if profile.department else "",
        )

        role_read = RoleRead(
            id=profile.role_info.id if profile.role_info else 0,
            name=profile.role_info.name if profile.role_info else user.role,
            description=profile.role_info.description if profile.role_info else "",
            permissions=profile.role_info.permissions if profile.role_info else [],
        )

        accounts_read: List[AccountRead] = [
            AccountRead(
                id=acc.id,
                account_number=acc.account_number,
                account_type=acc.account_type,
                balance_cents=acc.balance_cents,
                currency=acc.currency,
                status=acc.status,
                created_at=acc.created_at,
            )
            for acc in profile.accounts
        ]

        return UserProfileRead(
            id=user.id,
            name=user.name,
            email=user.email,
            role=user.role,
            phone_masked=masked_phone,
            department=dept_read,
            role_info=role_read,
            accounts=accounts_read,
            total_balance_cents=profile.total_balance_cents,
            account_count=len(profile.accounts),
            created_at=user.created_at or datetime.now(timezone.utc),
        )

    def _build_user_es_doc(self, user: User) -> Dict[str, Any]:
        """Constructs Elasticsearch document shape for User indexing."""
        masked_phone = None
        if self.crypto is not None and user.phone_encrypted:
            aad = f"users.phone:{user.id}".encode("utf-8")
            try:
                plain_phone = self.crypto.decrypt(user.phone_encrypted, aad=aad)
                masked_phone = self.crypto.mask_phone(plain_phone)
            except DecryptionError:
                masked_phone = "***"

        return {
            "id": str(user.id),
            "name": user.name,
            "email": user.email,
            "role": user.role,
            "department": user.department,
            "department_id": user.department_id or user.department,
            "role_id": user.role_id or user.role,
            "phone_masked": masked_phone,
            "created_at": user.created_at.isoformat() if user.created_at else None,
        }

    async def _build_user_es_payload(self, user_id: uuid.UUID) -> Dict[str, Any]:
        """Constructs both User and UserProfile documents for Outbox payload."""
        profile = await self.repo.get_profile(user_id)
        if profile is None:
            user = await self.repo.get(user_id)
            if not user:
                return {}
            return {"user": self._build_user_es_doc(user)}

        user = profile.user
        profile_read = self._to_profile_read(profile)

        user_doc = {
            "id": str(user.id),
            "name": user.name,
            "email": user.email,
            "role": user.role,
            "department": user.department,
            "department_id": user.department_id or user.department,
            "role_id": user.role_id or user.role,
            "phone_masked": profile_read.phone_masked,
            "account_count": profile_read.account_count,
            "total_balance_cents": profile_read.total_balance_cents,
            "created_at": user.created_at.isoformat() if user.created_at else None,
        }

        profile_doc = profile_read.model_dump(mode="json")
        profile_doc["department_name"] = profile_read.department.name if profile_read.department else None
        profile_doc["department_code"] = profile_read.department.code if profile_read.department else None
        profile_doc["role_name"] = profile_read.role_info.name if profile_read.role_info else None
        profile_doc["account_numbers"] = [a.account_number for a in profile_read.accounts]
        profile_doc["account_types"] = [a.account_type for a in profile_read.accounts]
        profile_doc["currencies"] = list(set(a.currency for a in profile_read.accounts))

        return {
            "user": user_doc,
            "profile": profile_doc,
        }

    async def _dispatch_outbox_sync(self, event: OutboxEvent) -> None:
        """Dispatches outbox event synchronization to Elasticsearch."""
        if self.outbox_worker is not None:
            asyncio.create_task(self.outbox_worker.sync_now(event.id))
        else:
            try:
                agg_id = str(event.aggregate_id)
                if event.event_type in ("CREATE", "UPDATE"):
                    payload = event.payload or {}
                    user_doc = payload.get("user") if isinstance(payload, dict) and "user" in payload else payload
                    if isinstance(user_doc, dict) and user_doc:
                        await self.es.index_document(index="user", id_=agg_id, document=user_doc)
                    profile_doc = payload.get("profile") if isinstance(payload, dict) else None
                    if isinstance(profile_doc, dict) and profile_doc:
                        await self.es.index_document(index="user_profile", id_=agg_id, document=profile_doc)
                elif event.event_type == "DELETE":
                    await self.es.delete_document(index="user", id_=agg_id)
                    await self.es.delete_document(index="user_profile", id_=agg_id)

                now = datetime.now(timezone.utc)
                await self.outbox_repo.mark_published(event.id, published_at=now)
            except Exception as exc:
                logger.warning("Direct outbox sync fallback failed: %s", exc)


# ------------------------------------------------------------------------------
# User Domain Dependencies (FastAPI Official Documentation Style)
# ------------------------------------------------------------------------------
def get_user_cache(
    redis: RedisDep,
    settings: Annotated[Settings, Depends(get_settings)],
) -> ICacheService:
    """Dependency constructing a DynamicRedisCache for the user domain."""
    if redis is not None:
        return DynamicRedisCache(
            redis=redis,
            prefix="users",
            item_ttl=settings.CACHE_ITEM_TTL,
            list_ttl=settings.CACHE_LIST_TTL,
            negative_ttl=settings.CACHE_NEGATIVE_TTL,
            lock_ttl_ms=settings.CACHE_LOCK_TTL_MS,
            flags=settings.get_cache_flags(),
        )
    return NullCache()


UserCacheDep = Annotated[ICacheService, Depends(get_user_cache)]


def get_user_service(
    session: SessionDep,
    cache: UserCacheDep,
    crypto: CryptoDep,
    es: ElasticsearchDep,
    request: Request,
) -> UserService:
    """Dependency composing database, Redis, AES-256-GCM, Outbox, and Elasticsearch into UserService."""
    worker = None
    if hasattr(request, "app") and hasattr(request.app, "state"):
        worker = getattr(request.app.state, "outbox_worker", None)

    return UserService(
        db=session,
        cache=cache,
        crypto=crypto,
        elasticsearch=es,
        outbox_worker=worker,
    )


UserServiceDep = Annotated[UserService, Depends(get_user_service)]


__all__ = [
    "UserService",
    "get_user_cache",
    "UserCacheDep",
    "get_user_service",
    "UserServiceDep",
]
