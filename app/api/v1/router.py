"""API v1 master router aggregating all domain endpoint modules."""

from fastapi import APIRouter

from app.api.v1.categories import router as categories_router
from app.api.v1.health import router as health_router
from app.api.v1.logs import router as logs_router
from app.api.v1.products import router as products_router
from app.api.v1.users import router as users_router

api_router = APIRouter()

api_router.include_router(users_router)
api_router.include_router(products_router)
api_router.include_router(categories_router)
api_router.include_router(logs_router)
api_router.include_router(health_router)

api_v1_router = api_router

__all__ = ["api_router", "api_v1_router"]
