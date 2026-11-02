"""FastAPI dependency providers: Session -> Cache -> Crypto -> Elasticsearch -> Service.

Clean Architecture boundary: Endpoints and Controllers only ever see domain Services.

Split into:
- `factory.py`: the `provide()` mechanism (session/cache/crypto/ES injection).
- `services.py`: the per-entity `get_x_service`/`XServiceDep` pairs that use it.
- `support.py`: the two dependencies that don't fit that shape (log search, temp state).

Re-exports the same public names the single-file module used to, at the same
import path (`app.api.deps`), so no call site elsewhere needs to change.
"""

from app.api.deps.factory import SessionDep, get_crypto, get_redis_client, provide
from app.api.deps.services import (
    CategoryServiceDep,
    EmployeeServiceDep,
    ProductServiceDep,
    UserServiceDep,
    get_category_service,
    get_employee_service,
    get_product_service,
    get_user_service,
)
from app.api.deps.support import (
    LogSearchServiceDep,
    TempStateDep,
    get_log_search_service,
    get_temp_state,
)

__all__ = [
    "provide",
    "SessionDep",
    "get_crypto",
    "get_redis_client",
    "get_user_service",
    "get_product_service",
    "get_category_service",
    "get_employee_service",
    "get_log_search_service",
    "get_temp_state",
    "UserServiceDep",
    "ProductServiceDep",
    "CategoryServiceDep",
    "EmployeeServiceDep",
    "LogSearchServiceDep",
    "TempStateDep",
]
