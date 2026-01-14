# Ledgerline Seeds

Seeds are versioned CSVs that provide Atlas reference data and sample feeds.

- In **development and CI**, seeds provide both reference data and sample feeds (CRM, catalog, usage, pricing). This lets the full pipeline run end-to-end without relying on live upstream systems.
- In **production**, seeds are disabled. Dynamic feeds come from true sources. Stable reference lists can remain seeds if the business wants that.

This approach keeps contracts consistent across environments, accelerates local iteration, and ensures problems are caught early before real data arrives.

---

## Lifecycle

1. **Seeds** are loaded into the warehouse as static tables via `dbt seed`.
2. **Sources** resolve runtime inputs.
   - In dev and CI, seeds land in `<target.schema>_seeds` and sources read from that schema.
   - In prod, sources read the raw ingestion schema and seeds are disabled.
3. **Staging** normalizes and deduplicates inputs.
   - In dev, staging queries seeds.
   - In prod, staging queries live ingestion tables.

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
- **Source, not seed** `atlas_meter_usage_daily`: unique combination `(customer_code, product_code, plan_code, report_date)` and freshness on `load_ts`.

These checks mirror real rules and keep bad shapes out before data moves downstream.

---

## Testing Strategy

Testing happens in three places:

1. **Seeds** (`seeds.yml`)
   - Uniqueness on keys, not nulls, foreign keys where applicable, simple value checks
   - Fail by default, no warning severities

2. **Sources**
   - Enforce true grain for usage and freshness on `load_ts`
   - Stop the build on failure

3. **Staging**
   - Remaining domain checks, sanity rules not feasible earlier, and dedupe where unavoidable
