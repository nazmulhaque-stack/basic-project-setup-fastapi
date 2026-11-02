"""The CRUDRouter class: composes the query-model builder and route factories.

    router = CRUDRouter(
        model=Product,
        service=get_product_service,
        create_schema=ProductCreate,
        read_schema=ProductRead,
        prefix="/products",
        filter_fields={"name": ["icontains"], "sku": ["eq"], "is_active": ["eq"]},
        sort_fields=["name", "price_cents", "created_at"],
    )

Generates:
    GET     /products              list (pagination + filters + sorting -> Page[ProductRead])
    GET     /products/{item_id}    get one by primary key (int or UUID)
    POST    /products              create (201)
    PATCH   /products/{item_id}    partial update (only sent fields)
    PUT     /products/{item_id}    full replace
    DELETE  /products/{item_id}    delete (204)
    GET     /products/search       full-text search offloaded to Elasticsearch (CQRS)

Which routes exist, and in what order they're registered, is driven entirely
by `ROUTE_BUILDERS` below — a class attribute, not a hardcoded method dispatch.
To add a new route kind (e.g. a bulk-delete endpoint) without touching this
file, subclass and extend the registry:

    class BulkCapableRouter(CRUDRouter):
        ROUTE_BUILDERS = {**CRUDRouter.ROUTE_BUILDERS, "bulk_delete": add_bulk_delete_route}

`routes=[...]` then accepts `"bulk_delete"` like any built-in route name.
"""

from collections.abc import Callable, Iterable, Mapping, Sequence
from typing import Any, ClassVar, Dict, List, Optional

from fastapi import APIRouter, params
from sqlalchemy import inspect as sa_inspect

from app.api.crud_router.query_model import ListQueryBase, build_list_query_model
from app.api.crud_router.routes import (
    add_create_route,
    add_delete_route,
    add_get_route,
    add_list_route,
    add_replace_route,
    add_update_route,
)
from app.api.crud_router.search import add_search_route
from app.schemas.base import Page, make_partial


class CRUDRouter(APIRouter):
    """Dynamic generic CRUD router generating typed OpenAPI endpoints for any SQLAlchemy entity."""

    ROUTE_BUILDERS: ClassVar[Dict[str, Callable[["CRUDRouter"], None]]] = {
        "list": add_list_route,
        "get": add_get_route,
        "create": add_create_route,
        "update": add_update_route,
        "replace": add_replace_route,
        "delete": add_delete_route,
        "search": add_search_route,
    }

    def __init__(
        self,
        *,
        model: type,
        service: Callable[..., Any],
        create_schema: type,
        read_schema: type,
        update_schema: Optional[type] = None,
        prefix: str,
        tags: Optional[List[str]] = None,
        filter_fields: Optional[Mapping[str, Sequence[str]]] = None,
        sort_fields: Optional[Sequence[str]] = None,
        routes: Optional[Iterable[str]] = None,
        route_dependencies: Optional[Mapping[str, Sequence[params.Depends]]] = None,
        **router_kwargs: Any,
    ) -> None:
        resource = model.__name__
        super().__init__(prefix=prefix, tags=tags or [resource], **router_kwargs)

        self.model = model
        self.resource = resource
        self.service_dep = service
        self.create_schema = create_schema
        self.update_schema = update_schema or make_partial(create_schema, name=f"{resource}Update")
        self.read_schema = read_schema
        self.page_schema = Page[read_schema]  # type: ignore[valid-type]

        # Determine Primary Key Python type (UUID, int, or str)
        pk_cols = sa_inspect(model).primary_key
        pk_col = pk_cols[0] if pk_cols else None
        try:
            self.id_type: type = pk_col.type.python_type if pk_col is not None else str
        except (NotImplementedError, AttributeError):
            self.id_type = str

        default_sort = ("created_at", "id") if "created_at" in sa_inspect(model).columns else ("id",)
        self.query_model = build_list_query_model(
            model, filter_fields or {}, sort_fields or default_sort
        )

        enabled = set(routes) if routes is not None else set(self.ROUTE_BUILDERS)
        unknown = (enabled | set(route_dependencies or {})) - set(self.ROUTE_BUILDERS)
        if unknown:
            raise ValueError(f"Unknown route name(s): {sorted(unknown)}. Use {tuple(self.ROUTE_BUILDERS)}.")
        self._deps = {k: list(v) for k, v in (route_dependencies or {}).items()}

        for name, builder in self.ROUTE_BUILDERS.items():
            if name in enabled:
                builder(self)

    def route_kwargs(self, name: str) -> Dict[str, Any]:
        """Shared `add_api_route()` kwargs (operation name + per-route dependencies).

        Public (not `_route_kwargs`) because the route-factory functions in
        `routes.py`/`search.py` are free functions, not methods, and need it.
        """
        return {"name": f"{self.resource.lower()}_{name}", "dependencies": self._deps.get(name)}


# Backward-compatible alias: the ordered tuple of built-in route names, derived
# from the registry above so the two can never drift out of sync.
ALL_ROUTES = tuple(CRUDRouter.ROUTE_BUILDERS)

__all__ = ["CRUDRouter", "ALL_ROUTES"]
