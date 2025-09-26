# Refined Layer

Refined presents **clean, current views** that feed marts and other downstream consumers. It collapses SCD-style history into the latest state per key and standardizes fact inputs so marts can focus on business logic, not cleanup.  

This layer is **materialized as `table`**. Persisting these relations caches commonly joined slices and creates stable contracts that multiple teams and models can safely build on.

---

## What Refined Contains

- **Usage**  
  - `ref_usage_atlas`: the latest valid `OPEN` usage per `usage_hkey`.  
  - Adds convenience metrics such as `overage_units = greatest(units_used - included_units, 0)`.  

- **Dimensions**  
  - `ref_customer_atlas`, `ref_product_atlas`, `ref_plan_atlas`, `ref_currency_atlas`, `ref_country_atlas`.  
  - Each is derived via `current_from_history` from its corresponding `hist_*` table.  
  - These contain exactly one row per surrogate key, plus lineage columns (`record_source`, `load_ts_utc`).  

Together these refined tables form the **standard contract** that marts use. Every fact joins against these dimension tables.

---

## Why Table Materialization

Refined models are **reused frequently**. Persisting them as tables has several benefits:

- Eliminates the cost of recomputing window functions across history.  
- Provides stable row counts for auditing and reconciliation.  
- Acts as a **clear boundary** between historical logs and presentation marts.  
- Improves query performance for BI tools by avoiding heavy joins into history.  

---

## Default Member Integrity

Each refined dimension carries forward the **default member** created in staging. Tests enforce:

- The default key `'-1'` exists (`has_default_key`).  
- Only one default row exists (`warn_on_multiple_default_key`).  
- The default row has `record_source = 'System.DefaultKey'` (`no_default_clash`).  

This guarantees marts can safely self-complete dimensions by cloning the default row whenever a fact references a new, not-yet-landed code.

---

## Hashing and Keys

- Refined dimensions **preserve surrogate keys from history**, such as `customer_hkey`.  
- No new hashing is introduced. If a row exists in history, refined simply passes it through.  
- In the marts layer, facts prefer these canonical keys. Only synthetic rows created during self-completion use a fallback deterministic hash over the business key.  

This separation ensures stable identities for historical rows while still allowing flexible handling of unseen keys.

---

## Testing Strategy

`refined.yml` defines:

- **Default-member integrity tests** (as above).  
- `not_null` and `unique` constraints on surrogate keys.  
- Nonnegativity checks on numeric fields like `units_used`, `included_units`, and `overage_units`.  

These tests are lightweight but effective at catching schema drift before it spreads into marts.

---

## Operational Notes

- Keep refined logic **thin and generic**. Temporal logic belongs in history, and heavy business rules belong in marts.  
- Add columns here only if they are **broadly reusable across many marts**. For example, `overage_units` is a general-purpose metric, while specialized billing calculations stay in usage marts.  
- Refined is a **reuse and stability layer**. It should be predictable and boring, which is exactly what downstream consumers need.  

---

## Why This Layer Matters

Refined is the **contract between raw historical logs and business-facing marts**. Without it, every mart would need to re-implement history collapse, default handling, and surrogate key lookups. By centralizing those responsibilities here, Ledgerline ensures:

- **Consistency** across marts and BI dashboards.  
- **Performance** by persisting clean, simple tables.  
- **Auditability** by preserving lineage fields.  
- **Simplicity** for analysts, who can query clean dimensions and facts without worrying about historical mechanics.  
