---
description: Verify dataclass models and their hand-written repositories
allowed-tools: Bash(python3 .agents/agents/developer-agent/skills/db_migration_helper.py), Read
---

1. Verify model health:
   ```bash
   python3 .agents/agents/developer-agent/skills/db_migration_helper.py
   ```
   Confirms every model inherits `app.models.base.Base`, is a plain dataclass with an explicit `id` field, has a `created_at` field for keyset pagination, and (for CRUD-exposed models) has a matching `*Repository` class under `app/repositories/`.
2. This project has no migration tool. If the check above is clean but the schema itself needs to change, write and run the SQL (`CREATE TABLE`/`ALTER TABLE`/...) directly against Postgres yourself, then update the matching dataclass model and hand-written repository SQL to match — ask for explicit confirmation before running anything against a shared database, since schema changes are hard to reverse.
