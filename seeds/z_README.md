# Ledgerline Seeds

Seeds are versioned CSVs that provide Atlas reference data and sample source feeds.  

- In **development and CI**, seeds provide both reference data and sample feeds (CRM, catalog, usage, pricing). This lets the full pipeline run end-to-end without relying on live upstream systems.  
- In **production**, dynamic feeds such as usage and pricing are replaced by true sources, while stable reference lists (countries, currencies, products, plans) may remain seeded.  

This approach keeps contracts consistent across environments, accelerates local iteration, and ensures that problems are caught early before real data arrives.

---

## Lifecycle

1. **Seeds** are loaded into the warehouse as static tables via `dbt seed`.  
2. **Sources** resolve runtime inputs.  
   - In dev/CI, seeds act as the `_seeds` schema.  
   - In prod, sources point to raw ingestion schemas.  
3. **Staging** normalizes inputs before history.  
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
- `atlas_currency_info`  
**Pricing**  
- `atlas_price_book_daily`  
**Sample Metering Feed**  
- `atlas_meter_usage_daily`

---

## Materialization and Lifecycle

Seeds are materialized as Snowflake tables in the `seeds` schema. A few rules apply:

- **Typing**: Each CSV specifies `+column_types` so Snowflake doesn’t guess. This keeps the schema predictable.  
- **Lineage**: A post-hook sets `load_ts` if it’s null, giving every row a load timestamp.  

---

## Contracts and Constraints

Seeds serve as the first contract layer of the pipeline. Each one declares basic uniqueness or combination tests in `seeds.yml`. This ensures that invalid shapes are caught early.

Key constraints:

- `atlas_crm_customer_info`: `customer_code` must be unique.  
- `atlas_catalog_product_info`: `product_code` must be unique.  
- `atlas_catalog_plan_info`: `plan_code` must be unique.  
- `atlas_currency_info`: `currency_code` must be unique.  
- `atlas_country_info`: `country_code` must be unique.  
- `atlas_price_book_daily`: unique combination `(product_code, plan_code, price_date)`.  
- `atlas_meter_usage_daily`: unique combination `(customer_code, product_code, plan_code, report_date)`.

These constraints mimic reality (customers must have one identity, usage must not double-count) and protect staging from broken inputs.

---

## Testing Strategy

Testing happens at two levels:

1. **Seeds** (`seeds.yml`)  
   - Uniqueness and combination constraints  
   - Enforce business key stability  
   - Keep reference tables internally consistent  

2. **Staging** (later in the pipeline)  
   - Stronger validations (nonnegative amounts, year >= 2000, not nulls)  
   - Deduplication at natural grain  

This two-tier strategy means cheap, broad tests at the seed stage and stricter, domain-aware tests in staging.