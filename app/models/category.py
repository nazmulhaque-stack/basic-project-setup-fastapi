"""Category row dataclass."""

from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING, List, Optional

from app.models.base import Base

if TYPE_CHECKING:
    from app.models.product import Product


@dataclass
class Category(Base):
    name: str
    id: Optional[int] = None  # SERIAL/IDENTITY: DB-assigned, omitted from INSERT
    description: Optional[str] = None
    is_active: bool = True
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    # Populated only by a repository method that explicitly fetches them.
    products: "List[Product]" = field(default_factory=list)

    def __repr__(self) -> str:
        return f"<Category id={self.id} name={self.name}>"


__all__ = ["Category"]
