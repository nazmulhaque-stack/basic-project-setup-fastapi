---
description: Verify SQLAlchemy models and Alembic readiness, then draft a migration
argument-hint: "describe_changes"
allowed-tools: Bash(python3 .agents/agents/developer-agent/skills/db_migration_helper.py), Bash(alembic *), Read
---

1. Verify model health:
   ```bash
   python3 .agents/agents/developer-agent/skills/db_migration_helper.py
   ```
   Confirms every model inherits `app.models.base.Base`, declares an explicit `__tablename__` and primary key, and indexes `created_at` for keyset pagination.
2. If clean, draft the migration:
   ```bash
   alembic revision --autogenerate -m "$ARGUMENTS"
   ```
3. Review the generated file under `alembic/versions/` before applying. Ask for explicit confirmation before running `alembic upgrade head` — it's a schema-mutating, hard-to-reverse operation.
