"""API v1 package: see app.api.v1.router for the auto-discovered route mount."""

from app.api.v1.router import api_router, api_v1_router

__all__ = ["api_router", "api_v1_router"]

