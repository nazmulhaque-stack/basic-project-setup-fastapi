"""Repositories package re-exporting the interface and concrete implementations."""

from app.repositories.base import BaseRepository
from app.repositories.category import CategoryRepository
from app.repositories.employee import EmployeeRepository
from app.repositories.interface import IBaseRepository
from app.repositories.product import ProductRepository
from app.repositories.user import UserRepository

__all__ = [
    "IBaseRepository",
    "BaseRepository",
    "UserRepository",
    "ProductRepository",
    "CategoryRepository",
    "EmployeeRepository",
]
