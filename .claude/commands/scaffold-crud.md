---
description: Scaffold a new CRUD domain feature (model, schema, repository, service, router)
argument-hint: --model <Name> --fields "field:type,..." [--dry-run]
allowed-tools: Bash(python3 .agents/agents/developer-agent/skills/scaffold_crud.py*), Read
---

Run the scaffold-crud skill for this repo's Repository/BaseService pattern:

```bash
python3 .agents/agents/developer-agent/skills/scaffold_crud.py $ARGUMENTS
```

This generates `app/models/<model>.py`, `app/schemas/<model>.py`, `app/repositories/<model>.py`, `app/services/<model>.py` (inherits `BaseService`), and `app/api/v1/<model_plural>.py` (dynamic `CRUDRouter`).

After scaffolding, open each generated file and fill in the domain-specific `before_create`/`before_update`/`to_read_schema` hooks per `app/services/user.py` — don't leave generic passthrough logic if the entity needs uniqueness checks, validation, or PII encryption.
