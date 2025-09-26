# Refined Layer

Refined presents clean, current views that feed marts and other downstream consumers. It collapses SCD-style history into the latest state per key and standardizes fact inputs so marts can focus on business logic, not cleanup.  

This layer is stored as regular tables. Keeping them persisted has several benefits discussed below.

---

## Contents of the Refined Layer

- **Usage**  
  `ref_usage_atlas` holds the latest valid usage rows, one per business key per day. It also adds convenience measures such as `overage_units`, which calculates usage beyond included units.

- **Dimensions**  
  `ref_customer_atlas`, `ref_product_atlas`, `ref_plan_atlas`, `ref_currency_atlas`, and `ref_country_atlas` each collapse their history tables into the current record per surrogate key. They also keep lineage fields like `record_source` and `load_ts_utc`.

Together these tables define the standard shapes that marts rely on. Every fact table joins to these dimensions, so they serve as the core contract for downstream analytics.

---

## What Refined Does and Does Not Do

**What it does**  
- Collapses SCD-style history into the latest row per key.  
- Publishes clean, stable dimension tables and usage snapshots.  
- Adds lightweight convenience fields like `overage_units`.  
- Serves as the contract for marts, so facts can always join cleanly.  

**What it does not do**  
- It does not create self-completing dimensions. That happens in marts.  
- It does not recompute historical logic. That stays in the history layer.  
- It does not embed heavy business rules like pricing. Those belong in marts.

Refined is intentionally thin. It’s the “current clean view,” not a place to implement business transformations.

---

## Why Table Materialization

Refined models are reused frequently. Persisting them as tables has several benefits:

- Eliminates the cost of recomputing window functions across history.  
- Provides stable row counts for auditing and reconciliation.  
- Acts as a clear boundary between historical logs and presentation marts.  
- Improves query performance for BI tools by avoiding heavy joins into history.  

---

## Default Member Integrity

Each refined dimension carries forward the default member created in staging. Tests enforce:

- The default key `'-1'` exists. (`has_default_key`)
- Only one default row exists. (`warn_on_multiple_default_key`)
- The default row is always marked with `record_source = 'System.DefaultKey'`. (`no_default_clash`)  

This guarantees marts can safely self-complete dimensions by cloning the default row whenever a fact references a new, not-yet-landed code.

---

## Testing Strategy

`refined.yml` defines:

- Default-member integrity tests (as above).  
- `not_null` and `unique` constraints on surrogate keys.  
- Nonnegativity checks on numeric fields like `units_used`, `included_units`, and `overage_units`.  

These tests are lightweight but effective at catching schema drift before it spreads into marts.