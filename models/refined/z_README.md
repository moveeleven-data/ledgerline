# Refined Layer

Refined is a semantic boundary: it publishes consistent interfaces (stable column names and `*_key` columns) between staging and marts. Refined models intentionally do not re-implement ingestion logic; they select from deduplicated staging and expose only what downstream consumers should rely on.

---

## What It Does

- Publishes a refined usage interface (`ref_usage_atlas`) at the feed grain (customer/product/plan/date), including `overage_units` and stable `*_key` columns.
- Publishes a refined price book interface (`ref_price_book_daily`) for pricing lookups by product/plan/date.
- Exposes a stable contract surface for marts by limiting columns to those intended for downstream use.

---

## What It Doesn’t Do

- No deduplication, “latest row” selection, or ingestion correctness logic (that belongs in staging).  
- No pricing logic or business-rule enrichment (that belongs in marts/facts).

---

## Materialization

Keep refined models lightweight (`view`) so they remain easy to audit and predictable to join from marts.
