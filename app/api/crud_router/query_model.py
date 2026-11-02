"""Dynamic filter/sort query-model builder for CRUDRouter.

Turns a `filter_fields`/`sort_fields` config into a validated Pydantic model
(`<Model>ListQuery`) at router-construction time, so every filter shows up in
`/docs` with its correct type and invalid query params are rejected with 422
instead of silently ignored.
"""

from collections.abc import Mapping, Sequence
from typing import Annotated, Any, Dict, List, Optional

from pydantic import BaseModel, BeforeValidator, ConfigDict, Field, create_model, field_validator
from sqlalchemy import inspect as sa_inspect

from app.repositories.base import FILTER_OPERATORS, LOOKUP_SEP

RESERVED_QUERY_NAMES = {"page", "size", "sort"}
LIST_OPERATORS = {"in", "not_in"}
TEXT_OPERATORS = {"contains", "icontains", "startswith", "endswith"}

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
    model: type,
    filter_fields: Mapping[str, Sequence[str]],
    sort_fields: Sequence[str],
) -> type:
    """Create `<Model>ListQuery` from the model's column types. Validates config at startup."""
    columns = sa_inspect(model).columns
    fields: Dict[str, Any] = {}

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
    unknown_sort = allowed_sort - set(columns.keys())
    if unknown_sort:
        raise ValueError(f"sort_fields not columns of {model.__name__}: {sorted(unknown_sort)}")

    def check_sort(cls: type, value: Optional[str]) -> Optional[str]:
        if value is None:
            return value
        bad = [k for k in value.split(",") if k.strip().lstrip("-+") not in allowed_sort]
        if bad:
            raise ValueError(f"Cannot sort by {bad}. Allowed: {sorted(allowed_sort)}")
        return value

    return create_model(
        f"{model.__name__}ListQuery",
        __base__=ListQueryBase,
        __validators__={"check_sort": field_validator("sort")(check_sort)},
        **fields,
    )


__all__ = [
    "RESERVED_QUERY_NAMES",
    "LIST_OPERATORS",
    "TEXT_OPERATORS",
    "OP_DESCRIPTIONS",
    "ListQueryBase",
    "build_list_query_model",
]
