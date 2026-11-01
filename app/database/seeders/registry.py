"""Explicit registry and dependency ordering for database seeders."""

from collections.abc import Iterable
from typing import TypeVar

from app.database.seeders.base import BaseSeeder

SeederType = TypeVar("SeederType", bound=type[BaseSeeder])
_SEEDERS: dict[str, type[BaseSeeder]] = {}


def register_seeder(seeder: SeederType) -> SeederType:
    name = seeder.name
    if not name or name in _SEEDERS:
        raise ValueError(f"Seeder name is empty or already registered: {name!r}")
    _SEEDERS[name] = seeder
    return seeder


def get_registered_seeders() -> dict[str, type[BaseSeeder]]:
    return dict(_SEEDERS)


def order_seeders(
    seeders: dict[str, type[BaseSeeder]],
    requested: Iterable[str] | None = None,
) -> list[type[BaseSeeder]]:
    """Topologically sort all seeders, or one seeder and its dependencies."""
    roots = list(requested) if requested is not None else sorted(seeders)
    ordered: list[type[BaseSeeder]] = []
    visiting: set[str] = set()
    visited: set[str] = set()

    def visit(name: str) -> None:
        if name not in seeders:
            raise ValueError(f"Unknown seeder or dependency: {name}")
        if name in visiting:
            raise ValueError(f"Circular seeder dependency detected at: {name}")
        if name in visited:
            return
        visiting.add(name)
        for dependency in seeders[name].dependencies:
            visit(dependency)
        visiting.remove(name)
        visited.add(name)
        ordered.append(seeders[name])

    for root in roots:
        visit(root)
    return ordered


__all__ = ["get_registered_seeders", "order_seeders", "register_seeder"]
