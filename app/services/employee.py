"""Employee Service implementing business logic, 1-to-1 constraints, and caching.

Inherits full CRUD, Redis caching, and Elasticsearch search from BaseService.
"""

from typing import Optional
import uuid

from sqlalchemy.ext.asyncio import AsyncSession

from app.core.elasticsearch import IElasticsearchService
from app.core.exceptions import ConflictError, NotFoundError
from app.models.employee import Employee
from app.models.user import User
from app.repositories.employee import EmployeeRepository
from app.schemas.employee import EmployeeCreate, EmployeeRead, EmployeeUpdate
from app.services.base import BaseService
from app.services.interface import ICacheService


class EmployeeService(BaseService[Employee, EmployeeCreate, EmployeeUpdate, EmployeeRead, uuid.UUID]):
    """Employee domain service orchestrating repository CRUD and 1-to-1 validation."""

    repository_class = EmployeeRepository

    def __init__(
        self,
        db: Optional[AsyncSession] = None,
        repo: Optional[EmployeeRepository] = None,
        cache: Optional[ICacheService] = None,
        elasticsearch: Optional[IElasticsearchService] = None,
        outbox_enabled: bool = False,
        auto_outbox_events: bool = False,
        outbox_topic: str = "domain.events",
        service_name: Optional[str] = None,
    ) -> None:
        resolved_repo: EmployeeRepository
        if repo is not None:
            resolved_repo = repo
        elif db is not None:
            resolved_repo = EmployeeRepository(db)
        else:
            raise ValueError("Either db session or repo must be provided to EmployeeService")

        super().__init__(
            repo=resolved_repo,
            read_schema_cls=EmployeeRead,
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
        self.repository: EmployeeRepository = resolved_repo

    async def before_create(self, payload: EmployeeCreate) -> Employee:
        """Validate 1-to-1 constraint and user existence before creating employee."""
        # 1. Check if user exists
        if self.db is not None:
            user = await self.db.get(User, payload.user_id)
            if user is None:
                raise NotFoundError(f"User with ID '{payload.user_id}' not found.")

        # 2. Check if an employee is already associated with this user_id
        existing = await self.repository.get_by_user_id(payload.user_id)
        if existing is not None:
            raise ConflictError(f"An employee is already assigned to user '{payload.user_id}'.")

        return await super().before_create(payload)

    async def before_update(self, entity: Employee, payload: EmployeeUpdate) -> None:
        """Validate 1-to-1 constraint if user_id is being updated."""
        update_data = payload.model_dump(exclude_unset=True)
        new_user_id = update_data.get("user_id")
        if new_user_id is not None and new_user_id != entity.user_id:
            if self.db is not None:
                user = await self.db.get(User, new_user_id)
                if user is None:
                    raise NotFoundError(f"User with ID '{new_user_id}' not found.")
            existing = await self.repository.get_by_user_id(new_user_id)
            if existing is not None and existing.id != entity.id:
                raise ConflictError(f"An employee is already assigned to user '{new_user_id}'.")

        await super().before_update(entity, payload)

    async def get_by_user_id(self, user_id: uuid.UUID) -> Optional[EmployeeRead]:
        """Fetch employee by user_id."""
        employee = await self.repository.get_by_user_id(user_id)
        if employee is None:
            return None
        return await self.to_read_schema(employee)


__all__ = ["EmployeeService"]
