# Ledgerline Seeds

This repo is intentionally **seeds-only** for portability: every input (CRM, catalog, country, pricing, and usage) is provided as a dbt seed so anyone can run `dbt build` end-to-end.

In a production system, the **metering feed** would typically be a dbt **source** (raw ingestion table with freshness + grain checks), while stable reference lists could remain seeds if desired.

---

## Lifecycle (portfolio)

1. **Seeds** are loaded into the warehouse as static tables via `dbt seed`.
2. **Staging** normalizes, casts, and deduplicates seeded inputs.
3. **Refined** publishes stable interfaces for marts.
4. **Marts** build contract-ready facts/dimensions.

---

## Purpose and Scope

Ledgerline ships seven seed datasets.

**CRM and Catalog**
- `atlas_crm_customer_info`
- `atlas_catalog_product_info`
- `atlas_catalog_plan_info`

**Reference Lists**
- `atlas_country_info`

**Pricing**
- `atlas_price_book_daily`

**Sample Metering Feed**
- `atlas_meter_usage_daily`

---

## Materialization and Lifecycle

Seeds are materialized as tables in `<target.schema>_seeds` in dev and CI. Seeds are disabled in prod. A few rules apply:

- **Typing**: Each CSV specifies `+column_types` so Snowflake doesn’t guess. This keeps the schema predictable.
- **Lineage**: A post-hook sets `load_ts` if it’s null, giving every row a load timestamp.

---

## Contracts and Constraints

Seeds are the first contract layer. Tests fail by default (no warning severities).

**Key constraints**

- `atlas_crm_customer_info`: `customer_code` unique and not null. `country_code` not null and must exist in `atlas_country_info`.
- `atlas_catalog_product_info`: `product_code` unique and not null.
- `atlas_catalog_plan_info`: `plan_code` unique and not null. `product_code` must exist in `atlas_catalog_product_info`. `billing_period` must be `monthly` or `annual`.
- `atlas_country_info`: `country_code` unique and not null.
- `atlas_price_book_daily`: unique combination `(product_code, plan_code, price_date)`. `unit_price` nonnegative. `product_code` and `plan_code` must exist in their catalog seeds.
- `atlas_meter_usage_daily` (seed in this repo): unique combination `(customer_code, product_code, plan_code, report_date)` enforced in staging.

These checks mirror real rules and keep bad shapes out before data moves downstream.

---

## Testing Strategy

Testing happens in two places:

1. **Seeds** (`seeds.yml`)
   - Types are pinned via `+column_types`
   - Basic shape checks where meaningful

2. **Staging** (`staging.yml`)
   - Grain uniqueness (e.g., one row per customer × product × plan × date)
   - Not nulls and relationships to seed reference tables
   - Range / accepted-values checks
