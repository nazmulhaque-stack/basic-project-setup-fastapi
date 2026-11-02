"""Employee repository implementing database operations and 1-to-1 user lookups."""

from typing import Optional
import uuid

from sqlalchemy import select

from app.models.employee import Employee
from app.repositories.base import SQLAlchemyRepository


class EmployeeRepository(SQLAlchemyRepository[Employee]):
    """Employee repository inheriting generic CRUD from SQLAlchemyRepository."""

    default_sort = ("name",)

    async def get_by_user_id(self, user_id: uuid.UUID) -> Optional[Employee]:
        """Fetch employee by associated user_id."""
        stmt = select(self.model).where(self.model.user_id == user_id)
        result = await self.session.execute(stmt)
        return result.scalars().first()


__all__ = ["EmployeeRepository"]
