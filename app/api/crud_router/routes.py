"""Core CRUD route-factory functions for CRUDRouter.

Each `add_*_route(router)` function registers exactly one endpoint on an
already-configured `CRUDRouter` instance. They are plain functions (not
methods) registered in `CRUDRouter.ROUTE_BUILDERS` precisely so a new route
kind can be added — by any caller, including a subclass — by registering one
more function in that dict, without editing this file or `CRUDRouter.__init__`.

IMPORTANT: do not add `from __future__ import annotations` to this file. Each
endpoint closure below (`list_items`, `get_item`, ...) is built at runtime
from a local type (`router.query_model`, `router.id_type`, ...); FastAPI reads
those annotations directly as real classes. Stringifying them would break
resolution, since names like `Q`/`IdAnnotation` only exist in this closure's
local scope, not at module level.
"""

from typing import TYPE_CHECKING, Annotated, Any

from fastapi import Body, Depends, Query, Response, status

from app.core.exceptions import NotFoundError

if TYPE_CHECKING:
    from app.api.crud_router.router import CRUDRouter


def add_list_route(router: "CRUDRouter") -> None:
    Q, ServiceDep = router.query_model, Annotated[Any, Depends(router.service_dep)]
    page_schema = router.page_schema

    async def list_items(q: Annotated[Q, Query()], service: ServiceDep) -> Any:  # type: ignore[valid-type]
        items, total = await service.list(
            filters=q.filters(), sort=q.sort_keys(), page=q.page, size=q.size
        )
        return page_schema.create(items, total=total, page=q.page, size=q.size)

    router.add_api_route(
        "",
        list_items,
        methods=["GET"],
        response_model=page_schema,
        summary=f"List {router.resource} (paginated, filterable, sortable)",
        **router.route_kwargs("list"),
    )


def add_get_route(router: "CRUDRouter") -> None:
    ServiceDep = Annotated[Any, Depends(router.service_dep)]
    IdAnnotation = router.id_type

    async def get_item(item_id: IdAnnotation, service: ServiceDep) -> Any:  # type: ignore[valid-type]
        item = await service.get(item_id)
        if item is None:
            raise NotFoundError(f"{router.resource} with ID '{item_id}' not found")
        return item

    router.add_api_route(
        "/{item_id}",
        get_item,
        methods=["GET"],
        response_model=router.read_schema,
        summary=f"Get {router.resource} by id",
        **router.route_kwargs("get"),
    )


def add_create_route(router: "CRUDRouter") -> None:
    Create, ServiceDep = router.create_schema, Annotated[Any, Depends(router.service_dep)]

    async def create_item(payload: Annotated[Create, Body()], service: ServiceDep) -> Any:  # type: ignore[valid-type]
        return await service.create(payload)

    router.add_api_route(
        "",
        create_item,
        methods=["POST"],
        response_model=router.read_schema,
        status_code=status.HTTP_201_CREATED,
        summary=f"Create {router.resource}",
        **router.route_kwargs("create"),
    )


def add_update_route(router: "CRUDRouter") -> None:
    Update, ServiceDep = router.update_schema, Annotated[Any, Depends(router.service_dep)]
    IdAnnotation = router.id_type

    async def update_item(
        item_id: IdAnnotation, payload: Annotated[Update, Body()], service: ServiceDep  # type: ignore[valid-type]
    ) -> Any:
        updated = await service.update(item_id, payload)
        if updated is None:
            raise NotFoundError(f"{router.resource} with ID '{item_id}' not found")
        return updated

    router.add_api_route(
        "/{item_id}",
        update_item,
        methods=["PATCH"],
        response_model=router.read_schema,
        summary=f"Partially update {router.resource}",
        **router.route_kwargs("update"),
    )


def add_replace_route(router: "CRUDRouter") -> None:
    Create, ServiceDep = router.create_schema, Annotated[Any, Depends(router.service_dep)]
    IdAnnotation = router.id_type

    async def replace_item(
        item_id: IdAnnotation, payload: Annotated[Create, Body()], service: ServiceDep  # type: ignore[valid-type]
    ) -> Any:
        updated = await service.update(item_id, payload)
        if updated is None:
            raise NotFoundError(f"{router.resource} with ID '{item_id}' not found")
        return updated

    router.add_api_route(
        "/{item_id}",
        replace_item,
        methods=["PUT"],
        response_model=router.read_schema,
        summary=f"Replace {router.resource}",
        **router.route_kwargs("replace"),
    )


def add_delete_route(router: "CRUDRouter") -> None:
    ServiceDep = Annotated[Any, Depends(router.service_dep)]
    IdAnnotation = router.id_type

    async def delete_item(item_id: IdAnnotation, service: ServiceDep) -> Response:  # type: ignore[valid-type]
        deleted = await service.delete(item_id)
        if not deleted:
            raise NotFoundError(f"{router.resource} with ID '{item_id}' not found")
        return Response(status_code=status.HTTP_204_NO_CONTENT)

    router.add_api_route(
        "/{item_id}",
        delete_item,
        methods=["DELETE"],
        status_code=status.HTTP_204_NO_CONTENT,
        response_class=Response,
        summary=f"Delete {router.resource}",
        **router.route_kwargs("delete"),
    )


__all__ = [
    "add_list_route",
    "add_get_route",
    "add_create_route",
    "add_update_route",
    "add_replace_route",
    "add_delete_route",
]
