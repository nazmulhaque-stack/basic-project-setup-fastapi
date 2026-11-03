#!/usr/bin/env python3
"""Automated CRUD Feature Scaffolder for Developer Agent.

Generates Model, Schemas, Repository, Service (extending BaseService),
and FastAPI Router conforming strictly to the repository architecture patterns.
"""

from __future__ import annotations

import argparse
from pathlib import Path
import sys
from typing import Dict, List

PROJECT_ROOT = Path(__file__).resolve().parents[4]


def parse_fields(fields_str: str) -> List[Dict[str, str]]:
    """Parses a comma-separated list of field_name:type definitions."""
    fields: List[Dict[str, str]] = []
    for pair in fields_str.split(","):
        pair = pair.strip()
        if not pair:
            continue
        if ":" in pair:
            name, ftype = pair.split(":", 1)
        else:
            name, ftype = pair, "str"
        fields.append({"name": name.strip(), "type": ftype.strip()})
    return fields


def generate_scaffold(entity_name: str, fields: List[Dict[str, str]], dry_run: bool = False) -> None:
    """Generates the code templates for Model, Schema, Repository, Service, and Endpoint."""
    lower_name = entity_name.lower()
    plural_name = f"{lower_name}s"
    class_name = entity_name.capitalize()

    print(f"Generating CRUD feature for: {class_name} ({plural_name})")

    # 1. Model Template (plain dataclass — no ORM; the real table is hand-written SQL
    # in the matching repository, e.g. via a one-off migration script or psql, since
    # this project has no migration tool)
    model_code = f'''"""Dataclass model for {class_name} (no ORM — hand-written asyncpg SQL elsewhere)."""

from dataclasses import dataclass
from datetime import datetime
from typing import Optional
import uuid

from app.models.base import Base


@dataclass
class {class_name}(Base):
    """Row shape for the `{plural_name}` table. Client-side-generates its own `id`
    (UUID) before insert — see `BaseService.before_create` — since there is no
    database-side default for it."""

    id: uuid.UUID
'''
    for f in fields:
        fname = f["name"]
        ftype = f["type"]
        py_type = "int" if ftype == "int" else ("bool" if ftype == "bool" else "str")
        model_code += f"    {fname}: {py_type}\n"

    model_code += "    created_at: Optional[datetime] = None\n"

    # 2. Schemas Template
    schema_code = f'''"""Pydantic V2 Data Transfer Objects for {class_name}."""

from datetime import datetime
from typing import List, Optional
import uuid

from pydantic import BaseModel, ConfigDict, Field


class {class_name}Base(BaseModel):
'''
    for f in fields:
        fname = f["name"]
        ftype = f["type"]
        py_type = "int" if ftype == "int" else ("bool" if ftype == "bool" else "str")
        schema_code += f"    {fname}: {py_type}\n"

    schema_code += f'''

class {class_name}Create({class_name}Base):
    pass


class {class_name}Update(BaseModel):
'''
    for f in fields:
        fname = f["name"]
        ftype = f["type"]
        py_type = "int" if ftype == "int" else ("bool" if ftype == "bool" else "str")
        schema_code += f"    {fname}: Optional[{py_type}] = None\n"

    schema_code += f'''

class {class_name}Read({class_name}Base):
    id: uuid.UUID
    created_at: datetime

    model_config = ConfigDict(from_attributes=True)


class {class_name}Page(BaseModel):
    items: List[{class_name}Read]
    next_cursor: Optional[str] = None
    limit: int
'''

    # 3. Repository Template: hand-written asyncpg SQL, no generic filter/operator
    # engine — this project deliberately dropped that genericity, so every
    # repository hand-writes its own WHERE/ORDER BY branches for exactly the
    # filter/sort fields its CRUDRouter declares. Fill in `_build_where` below.
    field_names = [f["name"] for f in fields]
    insert_cols = ", ".join(field_names)
    insert_placeholders = ", ".join(f"${i + 2}" for i in range(len(field_names)))
    update_set = ", ".join(f"{name} = ${i + 2}" for i, name in enumerate(field_names))
    entity_args = ",\n            ".join(f"entity.{name}" for name in field_names)
    row_fields = "\n        ".join(f'{name}=row["{name}"],' for name in field_names)

    repo_code = f'''"""Repository implementing hand-written asyncpg SQL for {class_name}."""

from collections.abc import Mapping, Sequence
from datetime import datetime
from typing import Any, List, Optional, Tuple
import uuid

from app.models.{lower_name} import {class_name}
from app.repositories.base import BaseRepository
from app.repositories.sql_helpers import keyset_seek_clause

_SORTABLE = {{"id", "created_at"}}


def _row_to_{lower_name}(row: Any) -> {class_name}:
    return {class_name}(
        id=row["id"],
        {row_fields}
        created_at=row["created_at"],
    )


class {class_name}Repository(BaseRepository[{class_name}, uuid.UUID]):
    """Concrete repository for the {class_name} entity."""

    @property
    def model(self) -> type[{class_name}]:
        return {class_name}

    async def get(self, entity_id: uuid.UUID) -> Optional[{class_name}]:
        row = await self.db.fetchrow("SELECT * FROM {plural_name} WHERE id = $1", entity_id)
        return _row_to_{lower_name}(row) if row else None

    async def create(self, entity: {class_name}) -> {class_name}:
        row = await self.db.fetchrow(
            """
            INSERT INTO {plural_name} (id, {insert_cols})
            VALUES ($1, {insert_placeholders})
            RETURNING *
            """,
            entity.id,
            {entity_args},
        )
        return _row_to_{lower_name}(row)

    async def update(self, entity: {class_name}) -> {class_name}:
        row = await self.db.fetchrow(
            """
            UPDATE {plural_name}
            SET {update_set}
            WHERE id = $1
            RETURNING *
            """,
            entity.id,
            {entity_args},
        )
        return _row_to_{lower_name}(row)

    async def delete(self, entity: {class_name}) -> None:
        await self.db.execute("DELETE FROM {plural_name} WHERE id = $1", entity.id)

    def _build_where(self, filters: Mapping[str, Any]) -> Tuple[str, List[Any]]:
        """Hand-write a clause here for each filter key this entity's CRUDRouter
        `filter_fields` config declares, e.g.:

            if filters.get("name__icontains") is not None:
                clauses.append(f"name ILIKE '%' || ${{len(params) + 1}} || '%'")
                params.append(filters["name__icontains"])
        """
        clauses: List[str] = []
        params: List[Any] = []
        where = f"WHERE {{' AND '.join(clauses)}}" if clauses else ""
        return where, params

    def _build_order_by(self, sort: Optional[Sequence[str]]) -> str:
        keys = list(sort or ("id",))
        order_parts: List[str] = []
        for key in keys:
            desc = key.startswith("-")
            col = key.lstrip("-+")
            if col in _SORTABLE:
                order_parts.append(f"{{col}} {{'DESC' if desc else 'ASC'}}")
        if "id" not in {{k.lstrip("-+") for k in keys}}:
            order_parts.append("id ASC")
        return "ORDER BY " + ", ".join(order_parts)

    async def list(
        self,
        *,
        filters: Optional[Mapping[str, Any]] = None,
        sort: Optional[Sequence[str]] = None,
        offset: int = 0,
        limit: int = 100,
    ) -> Sequence[{class_name}]:
        where, params = self._build_where(filters or {{}})
        order_by = self._build_order_by(sort)
        safe_limit = min(100, max(1, limit))
        query = f"SELECT * FROM {plural_name} {{where}} {{order_by}} OFFSET ${{len(params) + 1}} LIMIT ${{len(params) + 2}}"
        rows = await self.db.fetch(query, *params, max(0, offset), safe_limit)
        return [_row_to_{lower_name}(r) for r in rows]

    async def count(self, filters: Optional[Mapping[str, Any]] = None) -> int:
        where, params = self._build_where(filters or {{}})
        query = f"SELECT count(*) FROM {plural_name} {{where}}"
        return int(await self.db.fetchval(query, *params) or 0)

    async def list_keyset(
        self,
        limit: int = 50,
        cursor_dt: Optional[datetime] = None,
        cursor_id: Optional[Any] = None,
        order_col: str = "created_at",
        id_col: str = "id",
        **filters: Any,
    ) -> Tuple[List[{class_name}], bool]:
        where, params = self._build_where(filters)
        seek, seek_params = keyset_seek_clause(order_col, id_col, cursor_dt, cursor_id, len(params) + 1)
        if seek:
            where = f"{{where}} AND {{seek}}" if where else f"WHERE {{seek}}"
            params += seek_params

        safe_limit = min(100, max(1, limit))
        query = (
            f"SELECT * FROM {plural_name} {{where}} "
            f"ORDER BY {{order_col}} ASC, {{id_col}} ASC LIMIT ${{len(params) + 1}}"
        )
        rows = await self.db.fetch(query, *params, safe_limit + 1)
        has_next = len(rows) > safe_limit
        items = [_row_to_{lower_name}(r) for r in rows[:safe_limit]]
        return items, has_next


__all__ = ["{class_name}Repository"]
'''

    # 4. Service Template
    service_code = f'''"""Service implementing business rules and caching for {class_name}."""

import logging
from typing import Any, Dict, Optional
import uuid

from app.db.pool import UnitOfWork
from app.models.{lower_name} import {class_name}
from app.repositories.{lower_name} import {class_name}Repository
from app.schemas.{lower_name} import {class_name}Create, {class_name}Page, {class_name}Read, {class_name}Update
from app.services.base import BaseService
from app.services.interface import ICacheService

logger = logging.getLogger(__name__)


class {class_name}Service(BaseService[{class_name}, {class_name}Create, {class_name}Update, {class_name}Read, uuid.UUID]):
    """Domain service orchestrating repository CRUD and caching for {class_name}."""

    repository_class = {class_name}Repository

    def __init__(
        self,
        db: Optional[UnitOfWork] = None,
        repo: Optional[{class_name}Repository] = None,
        cache: Optional[ICacheService] = None,
        outbox_enabled: bool = False,
        auto_outbox_events: bool = False,
        outbox_topic: str = "domain.events",
        service_name: Optional[str] = None,
    ) -> None:
        resolved_repo: {class_name}Repository
        if repo is not None:
            resolved_repo = repo
        elif db is not None:
            resolved_repo = {class_name}Repository(db)
        else:
            raise ValueError("Either db session or repo must be provided to {class_name}Service")

        super().__init__(
            repo=resolved_repo,
            read_schema_cls={class_name}Read,
            cache=cache,
            order_col="created_at",
            id_col="id",
            db=db,
            outbox_enabled=outbox_enabled,
            auto_outbox_events=auto_outbox_events,
            outbox_topic=outbox_topic,
            service_name=service_name,
        )
        self.repository: {class_name}Repository = resolved_repo

    async def list_{plural_name}(
        self,
        limit: int = 50,
        cursor: Optional[str] = None,
        cache_enabled: Optional[bool] = None,
        **filters: Any,
    ) -> {class_name}Page:
        page_dict = await self.list_keyset(limit=limit, cursor=cursor, cache_enabled=cache_enabled, **filters)
        return {class_name}Page(**page_dict)

    # Reusable method aliases
    create_{lower_name} = BaseService.create
    get_{lower_name}_by_id = BaseService.get_by_id
    update_{lower_name} = BaseService.update_by_id
    delete_{lower_name} = BaseService.delete_by_id
'''

    # 5. Dynamic CRUDRouter Template (Zero explicit boilerplate)
    endpoint_code = f'''"""FastAPI dynamic CRUD router for {class_name}."""

from app.api.crud_router import CRUDRouter
from app.api.deps import provide
from app.models.{lower_name} import {class_name}
from app.schemas.{lower_name} import (
    {class_name}Create,
    {class_name}Read,
    {class_name}Update,
)
from app.services.{lower_name} import {class_name}Service

get_{lower_name}_service = provide({class_name}Service, cache_prefix="{plural_name}")

router = CRUDRouter(
    model={class_name},
    service=get_{lower_name}_service,
    create_schema={class_name}Create,
    update_schema={class_name}Update,
    read_schema={class_name}Read,
    prefix="/{plural_name}",
    tags=["{class_name}s"],
    filter_fields={{"name": ["eq", "icontains"]}},
    sort_fields=["id", "created_at"],
)

__all__ = ["router", "get_{lower_name}_service"]
'''

    targets = [
        (PROJECT_ROOT / f"app/models/{lower_name}.py", model_code),
        (PROJECT_ROOT / f"app/schemas/{lower_name}.py", schema_code),
        (PROJECT_ROOT / f"app/repositories/{lower_name}.py", repo_code),
        (PROJECT_ROOT / f"app/services/{lower_name}.py", service_code),
        (PROJECT_ROOT / f"app/api/v1/{plural_name}.py", endpoint_code),
    ]

    for path, content in targets:
        if dry_run:
            print(f"[DRY RUN] Would create: {path.relative_to(PROJECT_ROOT)} ({len(content)} bytes)")
        else:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(content, encoding="utf-8")
            print(f"✓ Generated: {path.relative_to(PROJECT_ROOT)}")


def main() -> int:
    parser = argparse.ArgumentParser(description="CRUD Feature Scaffolder")
    parser.add_argument("--model", required=True, help="Name of entity model (e.g. Order, Customer)")
    parser.add_argument(
        "--fields",
        default="name:str,description:str",
        help="Comma-separated field_name:type list (e.g. 'title:str,price:int,is_published:bool')",
    )
    parser.add_argument("--dry-run", action="store_true", help="Preview generated files without writing")
    args = parser.parse_args()

    fields = parse_fields(args.fields)
    generate_scaffold(args.model, fields, dry_run=args.dry_run)
    return 0


if __name__ == "__main__":
    sys.exit(main())
