# Refined Layer

Refined is a semantic boundary: it publishes consistent interfaces (stable column names and `*_key` columns) between staging and marts. Refined models intentionally do not re-implement ingestion logic; they select from deduplicated staging and expose only what downstream consumers should rely on.

---

## What It Does

- Publishes thin, stable dimensions (`ref_customer_atlas`, `ref_product_atlas`, `ref_plan_atlas`, `ref_currency_atlas`, `ref_country_atlas`) with consistent `*_key` naming.  
- Publishes a refined usage interface (`ref_usage_atlas`) at the feed grain (customer/product/plan/date), including a convenience metric `overage_units`.  
- Exposes a stable contract surface for marts by limiting columns to those intended for downstream use.

---

## What It Doesn’t Do

- No deduplication, “latest row” selection, or ingestion correctness logic (that belongs in staging).  
- No pricing logic or business-rule enrichment (that belongs in marts/facts).  
- No macro-driven frameworks; readability and explicit interfaces take priority.

---

## Materialization

Keep refined models lightweight (`view` or `table`) so they remain easy to audit and predictable to join from marts.
