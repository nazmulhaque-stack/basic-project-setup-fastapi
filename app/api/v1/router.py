"""API v1 master router aggregating all domain endpoint modules.

Adding a new entity's endpoint module (e.g. `app/api/v1/orders.py`) requires
one import + one `include_router()` call here, explicitly, as below. `health`
is mounted separately in `app/main.py`, outside the `/api/v1` prefix and the
Redis rate limiter, since liveness/readiness probes must never be versioned
or throttled.
"""

from fastapi import APIRouter

from app.api.v1.categories import router as categories_router
from app.api.v1.employees import router as employees_router
from app.api.v1.logs import router as logs_router
from app.api.v1.products import router as products_router
from app.api.v1.users import router as users_router

api_v1_router = APIRouter()

api_v1_router.include_router(users_router)
api_v1_router.include_router(products_router)
api_v1_router.include_router(categories_router)
api_v1_router.include_router(employees_router)
api_v1_router.include_router(logs_router)

__all__ = ["api_v1_router"]
