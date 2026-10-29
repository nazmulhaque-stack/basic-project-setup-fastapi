"""API v1 package consolidating domain routers and dynamic CRUD generators."""

from app.api.v1 import categories, health, logs, products, users
from app.api.v1.router import api_router, api_v1_router

__all__ = [
    "api_router",
    "api_v1_router",
    "categories",
    "health",
    "logs",
    "products",
    "users",
]

