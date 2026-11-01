"""Command-line tools for safe database inspection, migrations, and seeders."""

import argparse
import asyncio
import logging
import sys

from sqlalchemy import inspect, text

from app.core.config import get_settings
from app.database.initialization import (
    check_database_connection,
    create_missing_tables,
    get_missing_tables,
)
from app.database.seeders.runner import load_seeders, run_seeders
from app.database.seeders.registry import get_registered_seeders
from app.db.session import create_db_engine, create_session_factory

logger = logging.getLogger("app.database.cli")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m app.database",
        description="Inspect and safely manage the application database.",
    )
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ("status", "check", "create-tables", "migrate", "history"):
        commands.add_parser(name)
    revision = commands.add_parser("revision")
    revision.add_argument("message", nargs="+")
    rollback = commands.add_parser("rollback")
    rollback.add_argument("revision", nargs="?", default="-1")
    seed = commands.add_parser("seed")
    seed.add_argument("seed_args", nargs="*")
    return parser


def _print_missing(missing: list[str]) -> None:
    if not missing:
        print("[SUCCESS] All required tables exist.")
        return
    print("[WARNING] Missing tables detected:")
    for table in missing:
        print(f"- {table}")
    print("\nRun:")
    print("python -m app.database create-tables")
    print("\nOr apply migrations:")
    print("python -m app.database migrate")


async def _status() -> int:
    engine = create_db_engine()
    try:
        print("[INFO] Checking database connection...")
        await check_database_connection(engine)
        print("[SUCCESS] Database connection established.")
        print("\n[INFO] Checking database tables...")
        missing = await get_missing_tables(engine)
        _print_missing(missing)
        async with engine.connect() as connection:
            has_version_table = await connection.run_sync(
                lambda sync_connection: inspect(sync_connection).has_table(
                    "alembic_version"
                )
            )
            if has_version_table:
                version = await connection.scalar(
                    text("SELECT version_num FROM alembic_version")
                )
                print(f"\n[INFO] Current migration: {version}")
            else:
                print("\n[WARNING] Alembic migration history is not initialized.")
        return 1 if missing else 0
    finally:
        await engine.dispose()


async def _check() -> int:
    engine = create_db_engine()
    try:
        print("[INFO] Checking database connection...")
        await check_database_connection(engine)
        print("[SUCCESS] Database connection established.")
        missing = await get_missing_tables(engine)
        _print_missing(missing)
        return 1 if missing else 0
    finally:
        await engine.dispose()


async def _create_tables() -> int:
    engine = create_db_engine()
    try:
        print("[INFO] Checking database connection...")
        await check_database_connection(engine)
        print("[SUCCESS] Database connection established.")
        missing = await create_missing_tables(engine)
        if missing:
            print("[SUCCESS] Created missing tables:")
            for table in missing:
                print(f"- {table}")
        else:
            print("[SUCCESS] No missing tables; nothing was changed.")
        return 0
    finally:
        await engine.dispose()


async def _seed(seed_args: list[str]) -> int:
    load_seeders()
    seeders = get_registered_seeders()
    if seed_args and seed_args[0] == "list":
        print("[INFO] Available seeders:")
        for name in sorted(seeders):
            print(f"- {name}")
        return 0

    requested = seed_args[1:] if seed_args and seed_args[0] == "run" else seed_args
    if requested == ["all"]:
        requested = []
    engine = create_db_engine()
    try:
        await check_database_connection(engine)
        missing = await get_missing_tables(engine)
        if missing:
            _print_missing(missing)
            return 1
        completed = await run_seeders(create_session_factory(engine), requested or None)
        print(f"[SUCCESS] {len(completed)} seeder(s) executed successfully.")
        for name in completed:
            print(f"- {name}")
        return 0
    finally:
        await engine.dispose()


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    settings = get_settings()
    migration_url = settings.MIGRATION_DATABASE_URL or settings.DATABASE_URL

    try:
        if args.command == "status":
            return asyncio.run(_status())
        if args.command == "check":
            return asyncio.run(_check())
        if args.command == "create-tables":
            return asyncio.run(_create_tables())
        if args.command == "seed":
            return asyncio.run(_seed(args.seed_args))
        if args.command == "migrate":
            from app.database.migration import upgrade_database

            print("[INFO] Applying Alembic migrations...")
            upgrade_database(migration_url)
            print("[SUCCESS] Database is up to date.")
            return 0
        if args.command == "rollback":
            from app.database.migration import downgrade_database

            print(f"[INFO] Rolling back migration {args.revision}...")
            downgrade_database(migration_url, args.revision)
            print("[SUCCESS] Migration rollback completed.")
            return 0
        if args.command == "revision":
            from app.database.migration import create_revision

            message = " ".join(args.message)
            create_revision(migration_url, message)
            print(f"[SUCCESS] Created migration revision: {message}")
            return 0
        if args.command == "history":
            from app.database.migration import show_history

            show_history(migration_url)
            return 0
        parser.error(f"Unsupported command: {args.command}")
    except Exception as exc:
        logger.exception("Database command failed.")
        msg = str(exc)
        if "SASL authentication failed" in msg or "password authentication failed" in msg:
            print(
                "[ERROR] Database authentication failed (SASL / invalid password or user).\n"
                "Please check DB_USER and DB_PASSWORD (or DATABASE_URL) in your .env file.",
                file=sys.stderr,
            )
        else:
            print(f"[ERROR] {exc}", file=sys.stderr)
        return 1
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
