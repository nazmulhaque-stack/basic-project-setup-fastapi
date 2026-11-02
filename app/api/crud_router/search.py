"""CQRS full-text search route wiring for CRUDRouter.

Kept separate from `routes.py`'s CRUD route factories because it's a
conceptually distinct concern: it offloads to Elasticsearch (`service.search`)
rather than the SQLAlchemy repository, and has no filter/sort query model of
its own. A `CRUDRouter` subclass that doesn't need CQRS search can simply
omit `"search"` from `routes=`.

IMPORTANT: do not add `from __future__ import annotations` to this file — see
the note in `routes.py`; the same reasoning applies to `search_items` below.
"""

from typing import TYPE_CHECKING, Annotated, Any

from fastapi import Depends, Query

if TYPE_CHECKING:
    from app.api.crud_router.router import CRUDRouter


def add_search_route(router: "CRUDRouter") -> None:
    ServiceDep = Annotated[Any, Depends(router.service_dep)]

    async def search_items(
        service: ServiceDep,
        q: str = Query(..., description="Full-text search query string"),
        limit: int = Query(default=20, ge=1, le=100),
        offset: int = Query(default=0, ge=0),
    ) -> Any:
        docs, total = await service.search(query=q, limit=limit, offset=offset)
        return {"items": docs, "total": total, "query": q, "limit": limit, "offset": offset}

    router.add_api_route(
        "/search",
        search_items,
        methods=["GET"],
        summary=f"Full-text search {router.resource} via Elasticsearch",
        **router.route_kwargs("search"),
    )


__all__ = ["add_search_route"]
