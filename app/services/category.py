"""Category Service implementing catalog classification and caching.

Inherits full CRUD, Redis caching, and Elasticsearch search from BaseService with zero boilerplate.
"""

from typing import Optional
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.elasticsearch import IElasticsearchService
from app.models.category import Category
from app.repositories.category import CategoryRepository
from app.schemas.category import CategoryCreate, CategoryRead, CategoryUpdate
from app.services.base import BaseService
from app.services.interface import ICacheService


class CategoryService(BaseService[Category, CategoryCreate, CategoryUpdate, CategoryRead, int]):
    """Category domain service with automatic caching and repository orchestration."""

    repository_class = CategoryRepository


    def __init__(
        self,
        db: Optional[AsyncSession] = None,
        repo: Optional[CategoryRepository] = None,
        cache: Optional[ICacheService] = None,
        elasticsearch: Optional[IElasticsearchService] = None,
        outbox_enabled: bool = False,
        auto_outbox_events: bool = False,
        outbox_topic: str = "domain.events",
        service_name: Optional[str] = None,
    ) -> None:
        resolved_repo: CategoryRepository
        if repo is not None:
            resolved_repo = repo
        elif db is not None:
            resolved_repo = CategoryRepository(db)
        else:
            raise ValueError("Either db session or repo must be provided to CategoryService")

        super().__init__(
            repo=resolved_repo,
            read_schema_cls=CategoryRead,
            cache=cache,
            elasticsearch=elasticsearch,
            order_col="created_at",
            id_col="id",
            db=db,
            outbox_enabled=outbox_enabled,
            auto_outbox_events=auto_outbox_events,
            outbox_topic=outbox_topic,
            service_name=service_name,
        )
        self.repository: CategoryRepository = resolved_repo


__all__ = ["CategoryService"]
