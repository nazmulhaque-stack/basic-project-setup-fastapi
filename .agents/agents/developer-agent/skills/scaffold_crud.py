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

    # 1. Model Template
    model_code = f'''"""SQLAlchemy Declarative Model for {class_name}."""

from datetime import datetime, timezone
import uuid

from sqlalchemy import Boolean, DateTime, Integer, String
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.models.base import Base


class {class_name}(Base):
    """Database model representing a {lower_name}."""

    __tablename__ = "{plural_name}"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
    )
'''
    for f in fields:
        fname = f["name"]
        ftype = f["type"]
        if ftype == "int":
            model_code += f"    {fname}: Mapped[int] = mapped_column(Integer, nullable=False)\n"
        elif ftype == "bool":
            model_code += f"    {fname}: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)\n"
        else:
            model_code += f"    {fname}: Mapped[str] = mapped_column(String(255), nullable=False)\n"

    model_code += """    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        nullable=False,
        index=True,
    )
"""

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

    # 3. Repository Template
    repo_code = f'''"""Repository implementing data access for {class_name}."""

import uuid
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.{lower_name} import {class_name}
from app.repositories.base import BaseRepository


class {class_name}Repository(BaseRepository[{class_name}, uuid.UUID]):
    """Repository handling database queries for {class_name}."""

    def __init__(self, session: AsyncSession) -> None:
        super().__init__({class_name}, session)
'''

    # 4. Service Template
    service_code = f'''"""Service implementing business rules and caching for {class_name}."""

import logging
from typing import Any, Dict, Optional
import uuid

from sqlalchemy.ext.asyncio import AsyncSession

from app.models.{lower_name} import {class_name}
from app.repositories.{lower_name} import {class_name}Repository
from app.schemas.{lower_name} import {class_name}Create, {class_name}Page, {class_name}Read, {class_name}Update
from app.services.base import BaseService
from app.services.interface import ICacheService

logger = logging.getLogger(__name__)


class {class_name}Service(BaseService[{class_name}, {class_name}Create, {class_name}Update, {class_name}Read, uuid.UUID]):
    """Domain service orchestrating repository CRUD and caching for {class_name}."""

    def __init__(
        self,
        db: Optional[AsyncSession] = None,
        repo: Optional[{class_name}Repository] = None,
        cache: Optional[ICacheService] = None,
    ) -> None:
        resolved_repo = repo or ({class_name}Repository(db) if db else None)
        if resolved_repo is None:
            raise ValueError("Either db session or repo must be provided to {class_name}Service")

        super().__init__(
            repo=resolved_repo,
            read_schema_cls={class_name}Read,
            cache=cache,
            order_col="created_at",
            id_col="id",
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
