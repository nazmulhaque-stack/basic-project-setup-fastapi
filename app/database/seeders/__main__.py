"""CLI entrypoint for database seeder management: python -m app.database.seeders."""

import argparse
import asyncio
import sys

from app.database.initialization import check_database_connection, get_missing_tables
from app.database.seeders.registry import get_registered_seeders, order_seeders
from app.database.seeders.runner import load_seeders, run_seeders
from app.db.session import create_db_engine, create_session_factory


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m app.database.seeders",
        description="Inspect and execute database seeders.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    # list
    subparsers.add_parser("list", help="List all available seeders in dependency order.")

    # run [name]
    run_parser = subparsers.add_parser("run", help="Run all seeders or a specific seeder.")
    run_parser.add_argument("seeder_name", nargs="?", default=None, help="Specific seeder name to run.")

    return parser


def cmd_list() -> int:
    load_seeders()
    seeders = get_registered_seeders()
    ordered = order_seeders(seeders)
    print("\n[INFO] Available Database Seeders (Topological Execution Order):")
    for idx, seeder_cls in enumerate(ordered, start=1):
        deps = ", ".join(seeder_cls.dependencies) if seeder_cls.dependencies else "None"
        print(f"  {idx}. {seeder_cls.name} (Dependencies: {deps})")
    print("\nRun all seeders:     python -m app.database.seeders run")
    print("Run single seeder:   python -m app.database.seeders run <seeder_name>\n")
    return 0


async def cmd_run(seeder_name: str | None = None) -> int:
    load_seeders()
    engine = create_db_engine()
    try:
        print("[INFO] Checking database connection...")
        await check_database_connection(engine)
        print("[SUCCESS] Database connection established.")

        missing = await get_missing_tables(engine)
        if missing:
            print("[WARNING] Cannot seed because missing database tables exist:")
            for t in missing:
                print(f"  - {t}")
            print("\nPlease apply migrations or create tables first:")
            print("  python -m app.database migrate")
            return 1

        requested = [seeder_name] if seeder_name else None
        if requested:
            print(f"[INFO] Running specific seeder: {seeder_name}...")
        else:
            print("[INFO] Running all registered seeders in dependency order...")

        completed = await run_seeders(create_session_factory(engine), requested)
        print(f"[SUCCESS] {len(completed)} seeder(s) executed successfully:")
        for name in completed:
            print(f"  - {name}")
        return 0
    except Exception as exc:
        print(f"[ERROR] Seeding failed: {exc}")
        return 1
    finally:
        await engine.dispose()


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command == "list":
        return cmd_list()
    elif args.command == "run":
        return asyncio.run(cmd_run(args.seeder_name))
    else:
        parser.print_help()
        return 1


if __name__ == "__main__":
    sys.exit(main())
