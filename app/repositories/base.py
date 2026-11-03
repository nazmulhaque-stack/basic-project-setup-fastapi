"""Minimal shared repository plumbing: holds the active connection/transaction.

Unlike the SQLAlchemy-based version this replaces, there is no generic
dynamic filter/operator engine here — each concrete repository
(UserRepository, ProductRepository, ...) hand-writes its own SQL. This class
only provides the one thing every repository needs: the active
`UnitOfWork` (asyncpg connection + transaction), under both `self.db` and
`self.session` (the latter kept for call sites written against the old name).
"""

from typing import Generic, TypeVar

from app.db.pool import UnitOfWork
from app.models.base import Base
from app.repositories.interface import IBaseRepository

ModelType = TypeVar("ModelType", bound=Base)
IdType = TypeVar("IdType")


class BaseRepository(IBaseRepository[ModelType, IdType], Generic[ModelType, IdType]):
    """Shared connection plumbing for hand-written asyncpg repositories."""

    def __init__(self, db: UnitOfWork) -> None:
        self.db: UnitOfWork = db
        self.session: UnitOfWork = db


__all__ = ["BaseRepository"]
