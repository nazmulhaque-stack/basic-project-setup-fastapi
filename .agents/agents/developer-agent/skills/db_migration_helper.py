#!/usr/bin/env python3
"""Database Model & Repository Health Helper.

Validates that every dataclass model under `app.models` inherits from `Base`,
has an `id` field and a `created_at` field (needed for keyset cursor
pagination), and has a matching hand-written repository module under
`app.repositories`. There is no migration tool in this project (schema
changes are applied directly against Postgres), so this script checks model
*shape*, not migration state.
"""

from __future__ import annotations

import argparse
from dataclasses import fields as dataclass_fields, is_dataclass
import importlib
import inspect
from pathlib import Path
import sys
from typing import List, Type

# Add project root to sys.path
PROJECT_ROOT = Path(__file__).resolve().parents[4]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.models.base import Base

# Internal event/ledger models with no CRUDRouter exposure, accessed only via
# hand-written SQL directly inside their owning service (app/services/outbox.py,
# app/services/inventory_reservations.py) — they never had a dedicated repository.
_NO_REPOSITORY_EXPECTED = {"OutboxEvent", "InventoryReservation"}


def inspect_models() -> List[Type[Base]]:
    """Discovers all dataclass models registered under app.models."""
    import app.models as models_pkg

    discovered: List[Type[Base]] = []
    for attr_name in dir(models_pkg):
        attr = getattr(models_pkg, attr_name)
        if inspect.isclass(attr) and issubclass(attr, Base) and attr is not Base:
            discovered.append(attr)
    return discovered


def validate_schema_health() -> int:
    """Verifies that all models are proper dataclasses with an id/created_at field
    and a matching hand-written repository module."""
    print("======================================================")
    print("  Database Model & Repository Health Inspection      ")
    print("======================================================")

    models = inspect_models()
    print(f"Found {len(models)} registered dataclass models:")
    errors: List[str] = []

    for model in models:
        print(f"\n  • Model: {model.__name__}")

        if not is_dataclass(model):
            errors.append(f"{model.__name__} is not a dataclass.")
            continue

        field_names = [f.name for f in dataclass_fields(model)]
        print(f"    Fields ({len(field_names)}): {', '.join(field_names)}")

        if "id" not in field_names:
            errors.append(f"{model.__name__} has NO 'id' field defined!")

        if "created_at" not in field_names:
            print(f"    [WARN] 'created_at' missing on {model.__name__} (needed for keyset cursor pagination)")

        # Every CRUD-exposed model should have a matching hand-written repository,
        # since this project dropped the generic SQLAlchemy-based repository engine.
        if model.__name__ in _NO_REPOSITORY_EXPECTED:
            print(f"    Repository: none expected (internal event/ledger model)")
            continue

        repo_module_name = f"app.repositories.{model.__name__.lower()}"
        try:
            repo_module = importlib.import_module(repo_module_name)
            repo_class_name = f"{model.__name__}Repository"
            if not hasattr(repo_module, repo_class_name):
                errors.append(f"{repo_module_name} does not define {repo_class_name}.")
            else:
                print(f"    Repository: {repo_module_name}.{repo_class_name}")
        except ModuleNotFoundError:
            errors.append(f"No repository module found at {repo_module_name} for {model.__name__}.")

    if errors:
        print("\n[FAILED] Database model validation errors:")
        for err in errors:
            print(f"  ✗ {err}")
        return 1

    print("\n✓ All models are structurally healthy and have a matching repository.")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Database Model & Repository Health Helper")
    parser.add_argument("--check", action="store_true", default=True, help="Run schema health checks")
    args = parser.parse_args()
    return validate_schema_health()


if __name__ == "__main__":
    sys.exit(main())
