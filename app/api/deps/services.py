"""Pre-configured domain service dependency providers.

`_SERVICE_REGISTRY` is the single source of truth for each entity's service
class and Redis cache-key prefix (`cache:<prefix>:...`). Adding a new entity
means adding one row to that dict plus one `get_x_service`/`XServiceDep` pair
below — not duplicating the `provide(...)` call site from scratch.

The prefix is kept explicit here rather than derived automatically (e.g. by
pluralizing the class name) on purpose: `provide()`'s own naive fallback
(`CategoryService` -> `"category"`) would silently change the production
Redis key namespace for an entity whose plural isn't just "+s" — getting a
cache-key prefix wrong is a correctness bug, not a cosmetic one, so this
stays an explicit mapping instead of a guess.
"""

from typing import Annotated, Dict, Tuple, Type

from fastapi import Depends

from app.api.deps.factory import provide
from app.services.base import BaseService
from app.services.category import CategoryService
from app.services.employee import EmployeeService
from app.services.product import ProductService
from app.services.user import UserService

_SERVICE_REGISTRY: Dict[str, Tuple[Type[BaseService], str]] = {  # type: ignore[type-arg]
    "user": (UserService, "users"),
    "product": (ProductService, "products"),
    "category": (CategoryService, "categories"),
    "employee": (EmployeeService, "employees"),
}

get_user_service = provide(UserService, cache_prefix=_SERVICE_REGISTRY["user"][1])
get_product_service = provide(ProductService, cache_prefix=_SERVICE_REGISTRY["product"][1])
get_category_service = provide(CategoryService, cache_prefix=_SERVICE_REGISTRY["category"][1])
get_employee_service = provide(EmployeeService, cache_prefix=_SERVICE_REGISTRY["employee"][1])

UserServiceDep = Annotated[UserService, Depends(get_user_service)]
ProductServiceDep = Annotated[ProductService, Depends(get_product_service)]
CategoryServiceDep = Annotated[CategoryService, Depends(get_category_service)]
EmployeeServiceDep = Annotated[EmployeeService, Depends(get_employee_service)]


__all__ = [
    "get_user_service",
    "get_product_service",
    "get_category_service",
    "get_employee_service",
    "UserServiceDep",
    "ProductServiceDep",
    "CategoryServiceDep",
    "EmployeeServiceDep",
]
