# Macros

This folder holds reusable building blocks for Ledgerline. They keep models small, make behavior consistent, and centralize important choices such as hashing, history semantics, and default-key handling.  

---

## Folder Layout

- **`macros/core/`**  
  Small utilities such as date normalization and string handling.

- **`macros/tests/`**  
  Shared test macros used in YAML. They cover things like making sure default keys exist and validating numeric ranges.