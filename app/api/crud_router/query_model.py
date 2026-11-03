"""Dynamic filter/sort query-model builder for CRUDRouter.

Turns a `filter_fields`/`sort_fields` config into a validated Pydantic model
(`<Entity>ListQuery`) at router-construction time, so every filter shows up in
`/docs` with its correct type and invalid query params are rejected with 422
instead of silently ignored.

Type information comes from the entity's own Pydantic **read schema**
(`model.model_fields`) rather than any database/ORM introspection — repositories
are raw hand-written asyncpg SQL now, so there is no column metadata to inspect.
"""

from collections.abc import Mapping, Sequence
from typing import Annotated, Any, Dict, List, Optional, Type

from pydantic import BaseModel, BeforeValidator, ConfigDict, Field, create_model, field_validator

RESERVED_QUERY_NAMES = {"page", "size", "sort"}
LOOKUP_SEP = "__"
LIST_OPERATORS = {"in", "not_in"}
TEXT_OPERATORS = {"contains", "icontains", "startswith", "endswith"}
# The full operator vocabulary CRUDRouter's query-model builder understands.
# Repositories are free to support a subset of these per filter_fields entry —
# each hand-writes only the WHERE branches it actually declares.
FILTER_OPERATORS = {
    "eq", "ne", "gt", "gte", "lt", "lte", "in", "not_in",
    "like", "ilike", "contains", "icontains", "startswith", "endswith", "isnull",
}

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
    "endswith": "ends with",
    "isnull": "true = IS NULL, false = IS NOT NULL",
}


def _split_csv(value: Any) -> Any:
    """Accept both ?id__in=1&id__in=2 and ?id__in=1,2."""
    items = value if isinstance(value, list) else [value]
    out: List[Any] = []
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
    size: int = Field(default=20, ge=1, le=100, description="Items per page.")
    sort: Optional[str] = Field(
        default=None,
        description="Comma-separated fields; prefix with '-' for descending. e.g. -created_at,name",
        examples=["-created_at"],
    )

    def filters(self) -> Dict[str, Any]:
        return self.model_dump(exclude=RESERVED_QUERY_NAMES, exclude_none=True)

    def sort_keys(self) -> Optional[List[str]]:
        return [s.strip() for s in self.sort.split(",") if s.strip()] if self.sort else None


def build_list_query_model(
    read_schema: Type[BaseModel],
    filter_fields: Mapping[str, Sequence[str]],
    sort_fields: Sequence[str],
) -> type:
    """Create `<Entity>ListQuery` from the read schema's field types. Validates config at startup."""
    schema_fields = read_schema.model_fields
    fields: Dict[str, Any] = {}

    for field_name, ops in filter_fields.items():
        if field_name not in schema_fields:
            raise ValueError(f"{read_schema.__name__}.{field_name} is not a field (filter_fields).")
        if field_name in RESERVED_QUERY_NAMES:
            raise ValueError(f"'{field_name}' clashes with a reserved query param.")
        py_type: Any = schema_fields[field_name].annotation or str

        for op in ops:
            if op not in FILTER_OPERATORS:
                raise ValueError(f"Unknown operator '{op}' for {read_schema.__name__}.{field_name}.")
            param = field_name if op == "eq" else f"{field_name}{LOOKUP_SEP}{op}"
            if op in LIST_OPERATORS:
                annotation: Any = Annotated[Optional[List[py_type]], BeforeValidator(_split_csv)]
            elif op in TEXT_OPERATORS:
                annotation = Optional[str]
            elif op == "isnull":
                annotation = Optional[bool]
            else:
                annotation = Optional[py_type]
            fields[param] = (
                annotation,
                Field(default=None, description=f"{field_name} {OP_DESCRIPTIONS[op]}"),
            )

    allowed_sort = set(sort_fields)
    unknown_sort = allowed_sort - set(schema_fields.keys())
    if unknown_sort:
        raise ValueError(f"sort_fields not fields of {read_schema.__name__}: {sorted(unknown_sort)}")

    def check_sort(cls: type, value: Optional[str]) -> Optional[str]:
        if value is None:
            return value
        bad = [k for k in value.split(",") if k.strip().lstrip("-+") not in allowed_sort]
        if bad:
            raise ValueError(f"Cannot sort by {bad}. Allowed: {sorted(allowed_sort)}")
        return value

    return create_model(
        f"{read_schema.__name__}ListQuery",
        __base__=ListQueryBase,
        __validators__={"check_sort": field_validator("sort")(check_sort)},
        **fields,
    )


__all__ = [
    "RESERVED_QUERY_NAMES",
    "LOOKUP_SEP",
    "LIST_OPERATORS",
    "TEXT_OPERATORS",
    "FILTER_OPERATORS",
    "OP_DESCRIPTIONS",
    "ListQueryBase",
    "build_list_query_model",
]
