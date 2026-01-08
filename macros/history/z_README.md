# History Macros

This folder contains macros for persisting state over time. They support reference history (SCD-style), daily usage snapshots, and utilities for selecting the current version of a record.

---

## Scope

- Append new versions for reference data using a stable surrogate key and diff hash (SCD-style history).
- Select the latest version per key from history for refined “current” views.
- Standardize hashing and diff logic so keys remain deterministic across models.
- Provide utilities for “as-of” processing and incremental loading patterns.
