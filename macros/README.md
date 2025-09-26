# Macros Layer

The `macros/` folder holds **reusable building blocks** for Ledgerline. They keep models small, make behavior consistent, and centralize important choices such as hashing, history semantics, and default-key handling.  

Macros are the glue of the project: they let you write once, reuse everywhere, and enforce contracts in a consistent way.

---

## Folder Layout

- **`macros/core/`**  
  Small, pure utilities such as date normalization and string handling. These are foundational helpers that don’t depend on the Ledgerline schema.  

- **`macros/delivery/`**  
  Presentational utilities that prepare dimensions for safe delivery. Example: `self_completing_dimension`, which guarantees a fact always has a matching dimension row.  

- **`macros/dev_utils/`**  
  Local development helpers. Examples: inserting a single usage row for quick tests, or generating small synthetic datasets. These may have side effects and are scoped to dev use.  

- **`macros/history/`**  
  The history toolkit. Includes:  
  - Queries for latest prior state  
  - Synthetic close generators  
  - `current_from_history` selectors  
  - A parameterized `save_history` for SCD2 tables  

- **`macros/migrations/`**  
  Idempotent DDL utilities and wrappers to run versioned migrations safely from dbt.  

- **`macros/tests/`**  
  Generic tests used in YAML across layers, such as:  
  - Default-key checks  
  - Collision detection  
  - Numeric bound validations  

---

## Design Decisions

- **Deterministic hashing**  
  We use `dbt_utils.generate_surrogate_key` with explicit, ordered field lists.  
  - `usage_hkey` = surrogate identity of a daily usage bucket.  
  - `usage_hdiff` = detects value changes and guarantees synthetic CLOSE rows have a distinct version.  

- **Default member strategy**  
  Every dimension includes exactly one default row with:  
  - Key = `-1`  
  - `record_source = 'System.DefaultKey'`  
  Self-completion clones this row when a fact references an unseen code, ensuring facts never drop on joins.  

- **Closed vs. open domains**  
  - Currency is a **closed domain** and is not self-completed.  
  - Country, product, and plan are **open domains**. Codes may surface in usage before the catalog refresh lands, so these dimensions must self-complete.  

- **Incremental history**  
  History macros assume **append-only semantics** with merge behavior keyed by surrogate key, date, and row type. This preserves natural keys while allowing synthetic closes to represent churn.  

---

## How to Add a Macro

- Prefer **small, single-purpose macros**.  
- Use **descriptive parameter names** and make safe defaults explicit.  
- Keep macros **idempotent** where possible. Avoid side effects, except in clearly marked `dev_utils/` or `migrations/`.  
- Document each macro’s purpose and assumptions inline, so usage is unambiguous.  

---

## Why This Layer Matters

Macros are the **shared language of the project**. By consolidating hashing, default handling, and history semantics, Ledgerline ensures consistency across staging, history, refined, and marts. This consistency is what makes the pipeline maintainable, auditable, and predictable.
