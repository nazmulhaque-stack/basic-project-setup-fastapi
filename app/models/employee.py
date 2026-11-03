"""Employee row dataclass, 1-to-1 with User."""

from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Optional
import uuid

from app.models.base import Base

if TYPE_CHECKING:
    from app.models.user import User


@dataclass
class Employee(Base):
    id: uuid.UUID
    name: str
    age: int
    location: str
    user_id: uuid.UUID
    created_at: Optional[datetime] = None
    # Populated only by EmployeeRepository.get_with_user() (the hand-written
    # equivalent of the old `lazy="selectin"` relationship).
    user: "Optional[User]" = None

    def __repr__(self) -> str:
        return (
            f"<Employee id={self.id} name={self.name} age={self.age} "
            f"location={self.location} user_id={self.user_id}>"
        )


__all__ = ["Employee"]
