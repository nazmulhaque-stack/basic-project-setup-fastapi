"""Employees: generated CRUD router using CRUDRouter with Redis caching."""

from typing import Annotated

from fastapi import Depends

from app.api.crud_router import CRUDRouter
from app.api.deps import provide
from app.models.employee import Employee
from app.schemas.employee import EmployeeCreate, EmployeeRead, EmployeeUpdate
from app.services.employee import EmployeeService

# Cache-key prefix is explicit (not derived from the class name) so it never
# silently drifts: `cache:employees:...` is the production Redis namespace.
get_employee_service = provide(EmployeeService, cache_prefix="employees")
EmployeeServiceDep = Annotated[EmployeeService, Depends(get_employee_service)]

router = CRUDRouter(
    model=Employee,
    service=get_employee_service,
    create_schema=EmployeeCreate,
    update_schema=EmployeeUpdate,
    read_schema=EmployeeRead,
    prefix="/employees",
    tags=["Employees"],
    filter_fields={
        "name": ["eq", "icontains"],
        "location": ["eq", "icontains"],
        "age": ["eq", "gte", "lte"],
        "user_id": ["eq"],
    },
    sort_fields=["id", "name", "age", "location", "created_at"],
)

__all__ = ["router", "get_employee_service", "EmployeeServiceDep"]
