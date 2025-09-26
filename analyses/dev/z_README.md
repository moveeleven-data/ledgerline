# Dev Analyses

The `analyses/dev/` folder contains **developer scratch queries** that accelerate iteration. These files are experimental, temporary, and meant only for local use. They are versioned for sharing patterns but are **not part of the deployable project**.

---

## Scope

- **Macro validation**: quick probes to confirm core macros behave as intended.  
- **Synthetic data injection**: wrappers around macros like `dev_insert_usage` and `dev_delete_usage` to simulate or remove rows.  
- **Sandbox exploration**: joins, filters, and pivots that help explore edge cases without formalizing them into models.  

---

## Role in the Pipeline

Dev analyses are **development-only**. They may include destructive operations (inserts or deletes of synthetic rows), so they should **never run in production**. They are a sandbox for iteration, not an audit layer.  

If a probe proves valuable for reproducibility, promote it into `tests/` or `analyses/qa/`.

---

## Guidelines

- Keep queries **short, explicit, and disposable**.  
- Prefix filenames with `dev__` to distinguish them from QA probes.  
- Clean up synthetic rows after testing with `dev_delete_usage`.  
- Never rely on dev inserts/deletes for automated tests.  

---

## Current State

Examples include:  
- `dev__insert_missing_product_row.sql` to test dimension handling.  
- `dev__to_21st_century_date_test.sql` to validate date normalization logic.  

Over time, this folder will grow with more **ad-hoc developer utilities**, but the principle remains: **experiment locally, promote only what belongs in the pipeline**.
