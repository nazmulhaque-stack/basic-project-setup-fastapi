"""Marker base for domain row dataclasses (no ORM, no declarative metaclass).

Models are plain `@dataclass`es representing exactly the columns a query
returns. They are constructed by their repository from an `asyncpg.Record`
(or, before insert, from a Create-schema's `.model_dump()`); there is no
session, no lazy loading, and no identity map. Optional relationship fields
(e.g. `Product.category_rel`) default to `None` and are populated only by
repository methods that explicitly fetch them (the hand-written equivalent of
`selectinload`).
"""

from dataclasses import dataclass


@dataclass
class Base:
    """Marker base class for domain row dataclasses."""


__all__ = ["Base"]
