# Macros Layer

This folder holds reusable building blocks for Ledgerline. They keep models small, make behavior consistent, and centralize important choices such as hashing, history semantics, and default-key handling.  

---

## Folder Layout

- **`macros/core/`**  
  Small utilities such as date normalization and string handling.

- **`macros/delivery/`**  
  Presentational utilities that prepare dimensions for safe delivery. (`self_completing_dimensions`)

- **`macros/dev_utils/`**  
  Local development helpers. Examples: inserting a single usage row or generating small datasets. These may have side effects and are scoped to dev use.  

- **`macros/history/`**  
  Tools for managing change tracking. These include finding the latest prior row, creating synthetic closes, collapsing SCD histories into current views, and handling SCD2 saves.  

- **`macros/migrations/`**  
  Helpers for schema changes. They let you run versioned DDL safely through dbt, so migrations are repeatable and tracked in code.  

- **`macros/tests/`**  
  Shared test macros used in YAML. They cover things like making sure default keys exist, checking for hash collisions, and validating numeric ranges.