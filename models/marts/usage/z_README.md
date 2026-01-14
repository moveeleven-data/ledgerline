# Usage Mart

The Usage Mart is a lean star schema built on top of the refined interfaces. Dimensions are thin pass-throughs from staging (the contract and precision are enforced upstream to avoid drift).

---

## Models

- **Dims:** `dim_customer`, `dim_product`, `dim_plan`, `dim_country`
- **Facts:** `fact_daily_usage`
- **Operational metrics:** `profile_usage_daily`, `usage_anomalies_daily`

---

## Grain & Keys

Fact grain = `customer_key × product_key × plan_key × report_date`

Keys are deterministic hashes from natural keys. The fact can be produced without joining to dimensions; dims exist to provide descriptive attributes.

---

## Materialization

- Dims → tables (or views if you prefer lighter-weight, but keep them stable)
- Facts → table (recommended; pricing logic is non-trivial and queried repeatedly)
- EDA → tables (only when running windowed exploration)

---

## Testing

To avoid drift, precision and core constraints are enforced in staging. The mart layer relies on contracts and targeted fact-level checks:

- Fact: grain uniqueness on `(customer_key, product_key, plan_key, report_date)`
- Optional: relationships checks (only if you want downstream referential guarantees, not duplicates of staging rules)

---

## Layout

models/marts/usage/
- dim_customer.sql
- dim_product.sql
- dim_plan.sql
- dim_country.sql
- fact_daily_usage.sql
- profile_usage_daily.sql
- usage_anomalies_daily.sql
