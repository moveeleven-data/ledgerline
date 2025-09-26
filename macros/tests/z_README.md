# Tests Macros

The `macros/tests/` folder contains **generic test definitions** that can be applied from YAML across models. They encode cross-project rules once, so you don’t repeat custom SQL everywhere.

---

## Scope

- Provide reusable tests for **default-key integrity**, **hash stability**, and **numeric sanity checks**.  
- Keep rules **parameterized and explicit** so they adapt cleanly across staging, history, refined, and marts.  
- Reduce duplication: all logic for default handling, collisions, and bounds lives here rather than in one-off SQL.  

---

## Role in the Pipeline

Generic tests from this folder are attached in model YAML files and run with `dbt test`. They act as **lightweight governance checks** that enforce shared standards without cluttering models.

---

## Current State

The folder includes checks for default key presence and clashes, hash collision detection, and numeric bounds. As Ledgerline evolves, new generic tests should be added here to capture other **cross-cutting quality rules**.
