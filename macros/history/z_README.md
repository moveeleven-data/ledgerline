# History Macros

This folder contains the toolkit for persisting state over time. These macros define how Ledgerline captures change, generates synthetic closes, and selects current rows.

---

## Scope

- Encode SCD2 logic for reference data.  
- Generate synthetic closes so churn is explicit, not inferred.  
- Provide helpers for resolving the processing date and looking up prior opens.  
- Standardize hashing and diff logic so usage and reference history remain deterministic.  
- Support refined models with both current and historical views.