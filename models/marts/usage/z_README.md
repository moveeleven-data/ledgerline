# Usage Mart

The Usage Mart is a lean star schema built on top of the refined interfaces. Dimensions are thin pass-throughs from refined; the fact table contains the real pricing logic and publishes the contract-ready metrics.

---

## Models

- **Dims:** `dim_customer`, `dim_product`, `dim_plan`, `dim_currency`, `dim_country`
- **Facts:** `fact_usage`
- **EDA (optional):** `eda/int_fact_usage_priced_window`, `eda/fact_usage_window`

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

- Dims: `not_null`, `unique` on each `*_key`
- Fact: `not_null` on keys and metrics, grain uniqueness, and FK `relationships` to each dim key
- Business rules where meaningful (e.g., no negative usage, `overage_units >= 0`, `overage_share between 0 and 1`)

---

## Layout

models/marts/usage/
- dim_customer.sql
- dim_product.sql
- dim_plan.sql
- dim_currency.sql
- dim_country.sql
- fact_usage.sql

models/marts/usage/eda/
- int_fact_usage_priced_window.sql
- fact_usage_window.sql
