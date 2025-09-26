# Tests Layer

The `tests/` folder contains **singular tests**. These are custom SQL assertions that run with `dbt test`.  

Use singular tests when:
- A rule cannot be expressed with a generic test (e.g., `not_null`, `unique`).  
- You need multi-step probes with joins, windows, or custom date logic.  
- You want reproducible QA checks that live under version control.  

They **complement generic tests** declared in YAML across staging, history, refined, and marts.

---

## What These Tests Protect

- **Grain guarantees**  
  Example: `stg_usage_unique_per_day.sql` enforces one row per `(customer_code, product_code, plan_code, report_date)` in staging.  

- **History continuity**  
  Example: `hist_usage_prior_keys_present_on_asof.sql` asserts that any key `OPEN` before the as-of date must appear again on that date as either `OPEN` or `CLOSE_SYNTHETIC`.  

- **Quarantine boundaries**  
  Example: `ref_usage_no_invalid_leaks.sql` ensures rows flagged as invalid never leak into refined usage views.  

- **Pricing integrity**  
  Example:  
  - `pricing_coverage_threshold.sql` fails if fact coverage with `unit_price` drops below a threshold.  
  - `pricing_missing_rows.sql` lists the exact unpriced natural keys by day for investigation.  

- **Dimension coverage**  
  Example: `dim_product_no_missing_keys.sql` warns if facts reference product codes not in `dim_product`. This is a safety net on top of self-completing dimensions.  

---

## Severity and Tags

- Tests mix **`severity='error'`** for correctness breaks and **`severity='warn'`** for investigative signals.  
- Files that support audit workflows carry `tags: ['qa']`. This allows you to run just QA probes when needed.  

---

## Hashing and Keys in Tests

Where stable identity is needed, tests apply the **same hashing strategy as models** so results align with pipeline behavior:

- `usage_hkey = hash(customer_code, product_code, plan_code, report_date)`  
- `usage_hdiff = hash(customer_code, product_code, plan_code, report_date, units_used, included_units)`  

Synthetic closes pin `units_used = 0` and `included_units = 0` and set `report_date = as_of_date`, producing a distinct but deterministic `usage_hdiff`.  

By mirroring model logic, tests guarantee their results match how history and marts are built.

---

## Running Tests

- **Everything**:  
  ```bash
  dbt test
  ```

- **By folder**  
  ```bash
  dbt test --select path:tests
  ```

- **Only QA probes**
  ```bash
  dbt test --select tag:qa
  ```

## Best Practices

- Add new singular tests when you discover a rule that isn’t easily expressed as a generic test.  
- Keep them **short, deterministic, and explicit** about date logic.  
- Treat them as **guardrails, not patchwork** — they should catch bad data, not mask it.  

---

## Why This Layer Matters

Tests are the **safety net of the pipeline**. Generic YAML tests cover the basics, while singular tests enforce **business-critical rules** such as continuity, coverage, and pricing integrity.  

Together, they keep the Ledgerline pipeline **auditable, trustworthy, and stable**.
