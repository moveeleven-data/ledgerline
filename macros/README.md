# Macros

This folder holds reusable building blocks for Ledgerline. They keep models small, make behavior consistent, and centralize important choices such as hashing, history semantics, and default-key handling.  

---

## Folder Layout

- **`macros/core/`**  
  Small utilities such as date normalization and string handling.

- **`macros/dev_utils/`**  
  Local development helpers. Examples: inserting a single usage row or generating small datasets. These may have side effects and are scoped to dev use.  

- **`macros/migrations/`**  
  Helpers for schema changes. They let you run versioned DDL safely through dbt, so migrations are repeatable and tracked in code.  

- **`macros/tests/`**  
  Shared test macros used in YAML. They cover things like making sure default keys exist and validating numeric ranges.