# Dev Analyses

This folder contains developer scratch queries that accelerate iteration.

These files are experimental, temporary, and meant for local use. They are versioned for sharing patterns but are not part of the deployable project.

---

## Scope

- **Macro validation**: quick probes to confirm core macros behave as intended.  
- **Synthetic data injection**: wrappers around macros like `dev_insert_usage` and `dev_delete_usage` to simulate or remove rows.  
- **Sandbox exploration**: joins, filters, and pivots that help explore edge cases without formalizing them into models.  

---

## Current State

Examples include:  
- `dev__insert_missing_product_row.sql` to test dimension handling.  
- `dev__to_21st_century_date_test.sql` to validate date normalization logic.  

Over time, this folder will grow with more ad-hoc developer utilities, but the principle remains: experiment locally, promote only what belongs in the pipeline.
