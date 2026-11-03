---
name: db-migration
description: >-
  Inspects dataclass models under app.models, validates id/created_at fields, and checks each CRUD-exposed model has a matching hand-written asyncpg repository.
---

# Database Model & Repository Verification Skill

Use this skill when modifying or creating database models, or checking model/repository consistency. This project has **no migration tool** (Alembic was removed along with SQLAlchemy; schema changes are applied directly against Postgres by hand, outside of this codebase).

## Quick Execution

Run the model health helper script:
```bash
python3 .agents/agents/developer-agent/skills/db_migration_helper.py
```

## Procedures

1. **Verify Registered Models**:
   - Confirm that all models inherit from `app.models.base.Base` and are plain `@dataclass`es (no ORM).
   - Verify that each model has an explicit `id` field.
   - Ensure `created_at` is present for keyset cursor pagination.
   - Confirm every CRUD-exposed model has a matching hand-written repository under `app/repositories/` (internal event/ledger models like `OutboxEvent` and `InventoryReservation` are exempt — they're accessed only via raw SQL inside their owning service, never through a repository or CRUDRouter).

2. **Applying a schema change**: write and run the `ALTER TABLE`/`CREATE TABLE` SQL directly against Postgres (e.g. via `psql` or the project's own docker-compose Postgres container), then update the matching dataclass model and hand-written repository SQL to match. There is no autogenerate/upgrade step to run.
