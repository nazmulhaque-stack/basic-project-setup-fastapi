"""API v1 master router: auto-discovers and mounts every domain router module.

Adding a new entity's endpoint module (e.g. `app/api/v1/orders.py` exposing a
module-level `router: APIRouter`, as `CRUDRouter` produces) is picked up here
automatically — no edit to this file is required. This is the dynamic-CRUD
pattern's own "zero boilerplate" promise applied to route registration itself;
previously every new entity needed a manual `include_router()` call here too,
which had already drifted out of sync with `app/api/v1/__init__.py`'s module
list before this file existed.

`health` is deliberately excluded: it is mounted separately in `app/main.py`,
outside the `/api/v1` prefix and the Redis rate limiter, since liveness/
readiness probes must never be versioned or throttled.
"""

import importlib
import pkgutil
from pathlib import Path

from fastapi import APIRouter

# This module's own name, not the package's, so this file never needs to import
# the `app.api.v1` package __init__ (which itself imports from here) — avoids a
# circular import while still resolving "app.api.v1" dynamically from __name__.
_PACKAGE_NAME = __name__.rsplit(".", 1)[0]
_PACKAGE_DIR = Path(__file__).resolve().parent
_EXCLUDED_MODULES = {"health", "router"}

api_router = APIRouter()

for _module_info in sorted(pkgutil.iter_modules([str(_PACKAGE_DIR)]), key=lambda m: m.name):
    if _module_info.name in _EXCLUDED_MODULES or _module_info.name.startswith("_"):
        continue
    _module = importlib.import_module(f"{_PACKAGE_NAME}.{_module_info.name}")
    _discovered_router = getattr(_module, "router", None)
    if isinstance(_discovered_router, APIRouter):
        api_router.include_router(_discovered_router)

api_v1_router = api_router

__all__ = ["api_router", "api_v1_router"]
