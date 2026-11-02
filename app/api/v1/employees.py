"""Employees: generated CRUD router using CRUDRouter with Redis caching."""

from app.api.crud_router import CRUDRouter
from app.api.deps import get_employee_service
from app.models.employee import Employee
from app.schemas.employee import EmployeeCreate, EmployeeRead, EmployeeUpdate

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

__all__ = ["router", "get_employee_service"]
