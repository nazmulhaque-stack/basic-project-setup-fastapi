"""Repositories package re-exporting interfaces and concrete implementations."""

from app.repositories.base import BaseRepository, SQLAlchemyRepository
from app.repositories.category import CategoryRepository
from app.repositories.employee import EmployeeRepository
from app.repositories.interface import IBaseRepository
from app.repositories.product import ProductRepository
from app.repositories.user import UserRepository

__all__ = [
    "IBaseRepository",
    "BaseRepository",
    "SQLAlchemyRepository",
    "UserRepository",
    "ProductRepository",
    "CategoryRepository",
    "EmployeeRepository",
]
