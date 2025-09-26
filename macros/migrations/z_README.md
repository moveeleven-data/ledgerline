# Migrations Macros

The `macros/migrations/` folder contains utilities for managing **versioned schema changes**. These macros make structural changes reproducible, idempotent, and auditable through dbt.

---

## Scope

- Keep schema evolution **in code, not in wikis or manual steps**.  
- Make migrations **safe to run multiple times** (idempotent).  
- Parameterize database and schema so migrations work across environments.  
- Provide an orchestrator to run migrations in controlled order.  

---

## Role in the Pipeline

Migration macros are not part of daily model runs. They are run **intentionally** — for example, just before or after a deployment that renames or replaces models. This ensures schema changes are consistent, logged, and reviewable in version control.

---

## Current State

Right now this folder includes entry points like `run_migrations(...)` and versioned migration stubs (e.g. `V003_drop_table(...)`). Over time, new migration macros can be added sequentially, always following the same principle: **schema change as code**.
