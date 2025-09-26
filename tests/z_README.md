# Tests Layer

This folder contains singular tests. These are custom SQL assertions that run with `dbt test`.  

Use singular tests when:
- A rule cannot be expressed with a generic test (e.g., `not_null`, `unique`).  
- You need multi-step probes with joins, windows, or custom date logic.  
- You want reproducible QA checks that live under version control.  

They complement generic tests declared in YAML across staging, history, refined, and marts.

---

## What These Tests Protect

- **Grain guarantees** - Staging enforces one row per day. (`stg_usage_unique_per_day.sql`)  
- **History continuity** - If a key was active before the as-of date, it must still show up that day. (`hist_usage_prior_keys_present_on_asof.sql`)
- **Quarantine boundaries** - Invalid rows never leak into refined usage. (`ref_usage_no_invalid_leaks.sql`) 
- **Pricing integrity** - Coverage must stay above the threshold, and every natural key must have a price. (`pricing_coverage_threshold.sql`, `pricing_missing_rows.sql`)
- **Dimension coverage** - Facts must map to valid product codes. (`dim_product_no_missing_keys.sql`) 

---

## Severity and Tags  

Tests use **error severity** when a failure must stop the run and **warn severity** when the result is only a signal for further investigation.

Some tests also carry the `qa` tag so you can run just the QA probes when needed.  