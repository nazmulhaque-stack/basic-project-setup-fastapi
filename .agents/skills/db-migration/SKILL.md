---
name: db-migration
description: >-
  Inspects SQLAlchemy models, validates primary keys, table names, and verifies Alembic migration configuration and readiness.
---

# Database Migration & Schema Verification Skill

Use this skill when modifying or creating database models, verifying Alembic configuration, or checking schema health before generating database migrations.

## Quick Execution

Run the migration helper script:
```bash
python3 .agents/agents/developer-agent/skills/db_migration_helper.py
```

## Procedures

1. **Verify Registered Models**:
   - Confirm that all models inherit from `app.models.base.Base`.
   - Ensure `__tablename__` is explicitly set.
   - Verify that each model has an explicit primary key column.
   - Ensure `created_at` timestamp is indexed for Keyset cursor pagination.

2. **Generate New Migration**:
   ```bash
   alembic revision --autogenerate -m "describe_changes"
   ```

3. **Apply Pending Migrations**:
   ```bash
   alembic upgrade head
   ```
