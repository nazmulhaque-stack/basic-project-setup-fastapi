"""Shared SQL fragment for keyset (cursor) pagination.

This is *not* a generic filter-dispatch engine — each repository still
hand-writes its own WHERE clauses. It's just the identical
`(order_col, id_col) > (cursor_dt, cursor_id)` seek shape every entity's
keyset pagination uses, parameterized by column name and `$N` placeholder
offset so it isn't copy-pasted four times with the numbers hand-adjusted.
"""

from datetime import datetime
from typing import Any, List, Optional, Tuple


def keyset_seek_clause(
    order_col: str,
    id_col: str,
    cursor_dt: Optional[datetime],
    cursor_id: Optional[Any],
    param_offset: int,
) -> Tuple[str, List[Any]]:
    """Returns (sql_fragment, params) for the composite seek predicate, or
    ("", []) if no cursor was supplied (first page). `param_offset` is the
    index of the next available `$N` placeholder in the caller's query.
    """
    if cursor_dt is None or cursor_id is None:
        return "", []
    p1, p2 = param_offset, param_offset + 1
    fragment = f"({order_col} > ${p1} OR ({order_col} = ${p1} AND {id_col} > ${p2}))"
    return fragment, [cursor_dt, cursor_id]


__all__ = ["keyset_seek_clause"]
