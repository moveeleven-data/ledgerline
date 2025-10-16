# Refined Layer

Refined is the “current clean view.” It collapses history to the latest row per key and exposes simple, reusable tables that marts can join to—no extra logic.

---

## What It Does

- Collapses SCD history to the latest record per surrogate key.  
- Publishes thin dimensions (`ref_customer_atlas`, `ref_product_atlas`, `ref_plan_atlas`, `ref_currency_atlas`, `ref_country_atlas`) and a current usage snapshot (`ref_usage_atlas`).  
- Passes through basic lineage fields (`record_source`, `ingestion_ts`) from history.

---

## What It Doesn’t Do

- No business rules, pricing, or self-completing dimensions (those belong in marts).  
- No recomputing historical logic (that lives in the history layer).  
- No ref-level testing; input validation happens at sources, and output validation happens in marts.

---

## Materialization

Use light materialization (`view` or `table`) for speed and clarity.  
Keep it thin, stable, and easy to join from marts.
