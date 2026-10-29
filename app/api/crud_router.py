"""Dynamic CRUD router: one class call -> 6 documented, validated endpoints.

    router = CRUDRouter(
        model=Product,
        service=get_product_service,
        create_schema=ProductCreate,
        read_schema=ProductRead,
        prefix="/products",
        filter_fields={"name": ["icontains"], "price": ["gte", "lte"]},
        sort_fields=["name", "price"],
    )

Generates:
    GET     /products              list  (pagination + filters + sorting, Page[ProductRead])
    GET     /products/{item_id}    get one
    POST    /products              create (201)
    PATCH   /products/{item_id}    partial update (only sent fields)
    PUT     /products/{item_id}    full replace
    DELETE  /products/{item_id}    delete (204)

Filters are turned into a real Pydantic model at startup, so every filter appears in
/docs with its correct type, and typos (?prise__gte=1) are rejected with 422.

IMPORTANT: do not add `from __future__ import annotations` to this file. The endpoint
functions are created at runtime and FastAPI must see real classes in the annotations.
"""

from collections.abc import Callable, Iterable, Mapping, Sequence
from typing import Annotated, Any

from fastapi import APIRouter, Body, Depends, Query, Response, params, status
from pydantic import BaseModel, BeforeValidator, ConfigDict, Field, create_model, field_validator
from sqlalchemy import inspect as sa_inspect

from app.core.config import settings
from app.models.base import Base
from app.repositories.base import FILTER_OPERATORS, LOOKUP_SEP
from app.schemas.base import Page, make_partial
from app.services.base import BaseService

ALL_ROUTES = ("list", "get", "create", "update", "replace", "delete")
RESERVED_QUERY_NAMES = {"page", "size", "sort"}
LIST_OPERATORS = {"in", "not_in"}
TEXT_OPERATORS = {"contains", "icontains", "startswith"}

OP_DESCRIPTIONS = {
    "eq": "equals",
    "ne": "not equal to",
    "gt": "greater than",
    "gte": "greater than or equal to",
    "lt": "less than",
    "lte": "less than or equal to",
    "in": "any of (repeat the param or comma-separate: 1,2,3)",
    "not_in": "none of (repeat the param or comma-separate)",
    "contains": "contains text (case-sensitive)",
    "icontains": "contains text (case-insensitive)",
    "startswith": "starts with",
    "isnull": "true = IS NULL, false = IS NOT NULL",
}


def _split_csv(value: Any) -> Any:
    """Accept both ?id__in=1&id__in=2 and ?id__in=1,2."""
    items = value if isinstance(value, list) else [value]
    out: list[Any] = []
    for item in items:
        if isinstance(item, str):
            out.extend(part.strip() for part in item.split(",") if part.strip())
        else:
            out.append(item)
    return out


class ListQueryBase(BaseModel):
    """Common query params for every list endpoint. Filters are added dynamically."""

    model_config = ConfigDict(extra="forbid")  # unknown query params -> 422

    page: int = Field(default=1, ge=1, description="Page number (1-based).")
    size: int = Field(default=20, ge=1, le=settings.MAX_PAGE_SIZE, description="Items per page.")
    sort: str | None = Field(
        default=None,
        description="Comma-separated fields; prefix with '-' for descending. e.g. -price,name",
        examples=["-created_at"],
    )

    def filters(self) -> dict[str, Any]:
        return self.model_dump(exclude=RESERVED_QUERY_NAMES, exclude_none=True)

    def sort_keys(self) -> list[str] | None:
        return [s.strip() for s in self.sort.split(",") if s.strip()] if self.sort else None


def build_list_query_model(
    model: type[Base],
    filter_fields: Mapping[str, Sequence[str]],
    sort_fields: Sequence[str],
) -> type[ListQueryBase]:
    """Create `<Model>ListQuery` from the model's column types. Validates config at startup."""
    columns = sa_inspect(model).columns
    fields: dict[str, Any] = {}

    for field_name, ops in filter_fields.items():
        if field_name not in columns:
            raise ValueError(f"{model.__name__}.{field_name} is not a column (filter_fields).")
        if field_name in RESERVED_QUERY_NAMES:
            raise ValueError(f"'{field_name}' clashes with a reserved query param.")
        try:
            py_type: Any = columns[field_name].type.python_type
        except NotImplementedError:
            py_type = str

        for op in ops:
            if op not in FILTER_OPERATORS:
                raise ValueError(f"Unknown operator '{op}' for {model.__name__}.{field_name}.")
            param = field_name if op == "eq" else f"{field_name}{LOOKUP_SEP}{op}"
            if op in LIST_OPERATORS:
                annotation: Any = Annotated[list[py_type] | None, BeforeValidator(_split_csv)]
            elif op in TEXT_OPERATORS:
                annotation = str | None
            elif op == "isnull":
                annotation = bool | None
            else:
                annotation = py_type | None
            fields[param] = (
                annotation,
                Field(default=None, description=f"{field_name} {OP_DESCRIPTIONS[op]}"),
            )

    allowed_sort = set(sort_fields)
    unknown_sort = allowed_sort - set(columns.keys())
    if unknown_sort:
        raise ValueError(f"sort_fields not columns of {model.__name__}: {sorted(unknown_sort)}")

    def check_sort(cls: type, value: str | None) -> str | None:
        if value is None:
            return value
        bad = [k for k in value.split(",") if k.strip().lstrip("-+") not in allowed_sort]
        if bad:
            raise ValueError(f"Cannot sort by {bad}. Allowed: {sorted(allowed_sort)}")
        return value

    return create_model(  # type: ignore[call-overload,no-any-return]
        f"{model.__name__}ListQuery",
        __base__=ListQueryBase,
        __validators__={"check_sort": field_validator("sort")(check_sort)},
        **fields,
    )


class CRUDRouter(APIRouter):
    def __init__(
        self,
        *,
        model: type[Base],
        service: Callable[..., BaseService[Any, Any, Any]],
        create_schema: type[BaseModel],
        read_schema: type[BaseModel],
        update_schema: type[BaseModel] | None = None,
        prefix: str,
        tags: list[str] | None = None,
        filter_fields: Mapping[str, Sequence[str]] | None = None,
        sort_fields: Sequence[str] | None = None,
        routes: Iterable[str] = ALL_ROUTES,
        route_dependencies: Mapping[str, Sequence[params.Depends]] | None = None,
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
        self.query_model = build_list_query_model(
            model, filter_fields or {}, sort_fields or ("id", "created_at")
        )

        enabled = set(routes)
        unknown = (enabled | set(route_dependencies or {})) - set(ALL_ROUTES)
        if unknown:
            raise ValueError(f"Unknown route name(s): {sorted(unknown)}. Use {ALL_ROUTES}.")
        self._deps = {k: list(v) for k, v in (route_dependencies or {}).items()}

        for name in ALL_ROUTES:  # fixed order -> stable OpenAPI output
            if name in enabled:
                getattr(self, f"_add_{name}_route")()

    def _route_kwargs(self, name: str) -> dict[str, Any]:
        return {"name": f"{self.resource.lower()}_{name}", "dependencies": self._deps.get(name)}

    # Each method defines one endpoint. Local aliases make the annotations real classes.
    def _add_list_route(self) -> None:
        Q, ServiceDep = self.query_model, Annotated[BaseService, Depends(self.service_dep)]
        page_schema = self.page_schema

        async def list_items(q: Annotated[Q, Query()], service: ServiceDep) -> Any:  # type: ignore[valid-type]
            items, total = await service.list(
                filters=q.filters(), sort=q.sort_keys(), page=q.page, size=q.size
            )
            return page_schema.create(items, total=total, page=q.page, size=q.size)

        self.add_api_route(
            "", list_items, methods=["GET"], response_model=page_schema,
            summary=f"List {self.resource} (paginated, filterable, sortable)",
            **self._route_kwargs("list"),
        )

    def _add_get_route(self) -> None:
        ServiceDep = Annotated[BaseService, Depends(self.service_dep)]

        async def get_item(item_id: int, service: ServiceDep) -> Any:
            return await service.get(item_id)

        self.add_api_route(
            "/{item_id:int}", get_item, methods=["GET"], response_model=self.read_schema,
            summary=f"Get {self.resource} by id", **self._route_kwargs("get"),
        )

    def _add_create_route(self) -> None:
        Create, ServiceDep = self.create_schema, Annotated[BaseService, Depends(self.service_dep)]

        async def create_item(payload: Annotated[Create, Body()], service: ServiceDep) -> Any:  # type: ignore[valid-type]
            return await service.create(payload)

        self.add_api_route(
            "", create_item, methods=["POST"], response_model=self.read_schema,
            status_code=status.HTTP_201_CREATED, summary=f"Create {self.resource}",
            **self._route_kwargs("create"),
        )

    def _add_update_route(self) -> None:
        Update, ServiceDep = self.update_schema, Annotated[BaseService, Depends(self.service_dep)]

        async def update_item(
            item_id: int, payload: Annotated[Update, Body()], service: ServiceDep  # type: ignore[valid-type]
        ) -> Any:
            return await service.update(item_id, payload, partial=True)

        self.add_api_route(
            "/{item_id:int}", update_item, methods=["PATCH"], response_model=self.read_schema,
            summary=f"Partially update {self.resource}", **self._route_kwargs("update"),
        )

    def _add_replace_route(self) -> None:
        Create, ServiceDep = self.create_schema, Annotated[BaseService, Depends(self.service_dep)]

        async def replace_item(
            item_id: int, payload: Annotated[Create, Body()], service: ServiceDep  # type: ignore[valid-type]
        ) -> Any:
            return await service.update(item_id, payload, partial=False)

        self.add_api_route(
            "/{item_id:int}", replace_item, methods=["PUT"], response_model=self.read_schema,
            summary=f"Replace {self.resource}", **self._route_kwargs("replace"),
        )

    def _add_delete_route(self) -> None:
        ServiceDep = Annotated[BaseService, Depends(self.service_dep)]

        async def delete_item(item_id: int, service: ServiceDep) -> Response:
            await service.delete(item_id)
            return Response(status_code=status.HTTP_204_NO_CONTENT)

        self.add_api_route(
            "/{item_id:int}", delete_item, methods=["DELETE"],
            status_code=status.HTTP_204_NO_CONTENT, response_class=Response,
            summary=f"Delete {self.resource}", **self._route_kwargs("delete"),
        )
