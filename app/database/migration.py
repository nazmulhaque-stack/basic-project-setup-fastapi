"""Alembic command helpers used by the database CLI and optional startup migration."""

from contextlib import contextmanager
import os
from pathlib import Path
from typing import Iterator

from alembic import command
from alembic.config import Config

from app.db.session import normalize_async_database_url

PROJECT_ROOT = Path(__file__).resolve().parents[2]
ALEMBIC_CONFIG_PATH = PROJECT_ROOT / "alembic.ini"


@contextmanager
def _migration_environment(database_url: str) -> Iterator[Config]:
    url = normalize_async_database_url(database_url)
    previous_url = os.environ.get("MIGRATION_DATABASE_URL")
    os.environ["MIGRATION_DATABASE_URL"] = url
    try:
        yield Config(str(ALEMBIC_CONFIG_PATH))
    finally:
        if previous_url is None:
            os.environ.pop("MIGRATION_DATABASE_URL", None)
        else:
            os.environ["MIGRATION_DATABASE_URL"] = previous_url


def upgrade_database(database_url: str, revision: str = "head") -> None:
    """Apply existing migrations; never creates or edits migration scripts."""
    with _migration_environment(database_url) as config:
        command.upgrade(config, revision)


def downgrade_database(database_url: str, revision: str = "-1") -> None:
    with _migration_environment(database_url) as config:
        command.downgrade(config, revision)


def create_revision(database_url: str, message: str, *, autogenerate: bool = True) -> None:
    with _migration_environment(database_url) as config:
        command.revision(config, message=message, autogenerate=autogenerate)


def show_history(database_url: str) -> None:
    with _migration_environment(database_url) as config:
        command.history(config)


__all__ = [
    "create_revision",
    "downgrade_database",
    "show_history",
    "upgrade_database",
]
