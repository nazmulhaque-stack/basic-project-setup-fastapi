"""Dynamic CRUD router: one class call -> documented, validated endpoints.

Split into composable pieces for easier extension:
- `query_model.py`: the dynamic filter/sort Pydantic query-model builder.
- `routes.py`: the core CRUD route factories (list/get/create/update/replace/delete).
- `search.py`: the CQRS full-text-search route wiring.
- `router.py`: the `CRUDRouter` class, composing the above via `ROUTE_BUILDERS`.

This re-exports the same public names the single-file module used to, at the
same import path (`app.api.crud_router`), so no call site elsewhere needs to change.
"""

from app.api.crud_router.query_model import ListQueryBase, build_list_query_model
from app.api.crud_router.router import ALL_ROUTES, CRUDRouter

__all__ = [
    "CRUDRouter",
    "ListQueryBase",
    "build_list_query_model",
    "ALL_ROUTES",
]
