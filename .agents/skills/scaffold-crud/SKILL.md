---
name: scaffold-crud
description: >-
  Scaffolds a complete, production-grade CRUD feature (Model, Schemas, Repository, BaseService inheritance, and FastAPI endpoints) with zero boilerplate.
---

# CRUD Feature Scaffolding Skill

Use this skill when you need to create a new domain entity or resource in the project conforming to the Repository Pattern and BaseService architecture.

## Usage

```bash
python3 .agents/agents/developer-agent/skills/scaffold_crud.py --model <ModelName> --fields "<field1:type,field2:type>"
```

### Options
- `--model`: Singular capitalized model name (e.g., `Order`, `Invoice`, `Customer`).
- `--fields`: Comma-separated list of `field_name:type` (supported types: `str`, `int`, `bool`).
- `--dry-run`: Preview generated files without writing to disk.

### Example
```bash
python3 .agents/agents/developer-agent/skills/scaffold_crud.py \
  --model Order \
  --fields "order_number:str,amount_cents:int,is_paid:bool"
```

## Generated Files
1. `app/models/<model>.py`
2. `app/schemas/<model>.py`
3. `app/repositories/<model>.py`
4. `app/services/<model>.py` (Inherits from `BaseService`)
5. `app/api/v1/endpoints/<model>.py`
