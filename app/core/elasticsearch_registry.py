"""Declarative registry of which database tables auto-sync to which
Elasticsearch index.

`BaseRepository.insert`/`update_row`/`delete_row` (in `app/repositories/base.py`)
consult this registry, keyed by table name, to decide whether a write should
enqueue an Elasticsearch sync event at all, and if so under which index name
and primary key column. There is deliberately no SQL/schema introspection
here — this is explicit, hand-maintained metadata, per the project rule
against building an unsafe generic engine that assumes it can infer sync
behavior from arbitrary SQL.

The `index` values match the singular model name (`self.resource_name.lower()`
in `app/services/base.py`) that `BaseService.search()` already queries via
`CRUDRouter`'s `/search` endpoint — not a new `<table>_v1` naming scheme —
so documents written by the auto-sync path are found by the search path
that already exists. If a future reindex needs a versioned index name
(`product_v2`), add an alias or update both sides of this registry together.
"""

from typing import Dict, Optional, TypedDict


class IndexConfig(TypedDict, total=False):
    index: str
    primary_key: str
    enabled: bool
    auto_sync: bool
    soft_delete: bool


ELASTICSEARCH_INDEXES: Dict[str, IndexConfig] = {
    "users": {
        "index": "user",
        "primary_key": "id",
        "enabled": True,
        "auto_sync": True,
        "soft_delete": False,
    },
    "products": {
        "index": "product",
        "primary_key": "id",
        "enabled": True,
        "auto_sync": True,
        "soft_delete": False,
    },
    "categories": {
        "index": "category",
        "primary_key": "id",
        "enabled": True,
        "auto_sync": True,
        "soft_delete": False,
    },
    "employees": {
        "index": "employee",
        "primary_key": "id",
        "enabled": True,
        "auto_sync": True,
        "soft_delete": False,
    },
}


def get_index_config(table: str) -> Optional[IndexConfig]:
    """Returns the registered Elasticsearch sync config for `table`, or `None`
    if that table has no registered index (auto-sync is then a safe no-op)."""
    return ELASTICSEARCH_INDEXES.get(table)


__all__ = ["ELASTICSEARCH_INDEXES", "IndexConfig", "get_index_config"]
