from fastapi import APIRouter

from app.api.v1 import categories, products

api_router = APIRouter()
api_router.include_router(categories.router)
api_router.include_router(products.router)
