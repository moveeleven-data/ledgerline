# Ledgerline Seeds

Seeds are **small, slow-changing CSVs** that bootstrap Atlas reference data and provide sample source feeds. They allow Ledgerline to run end-to-end analytics even when upstream systems are unavailable. In development and CI, these CSVs **simulate external sources** like CRM or metering. In production, most seeds are replaced by real ingestion pipelines, but some **reference seeds may remain authoritative** (for example, country codes or currency formats).

Think of seeds as scaffolding: they give you the shape of the data contracts you need, let you build transformations against a fixed dataset, and catch problems early before real data arrives.

---

## Purpose and Scope

Ledgerline ships seven seed datasets, grouped by functional role:

- **CRM and Catalog**  
  - `atlas_crm_customer_info` (customers)  
  - `atlas_catalog_product_info` (products)  
  - `atlas_catalog_plan_info` (plans)  
- **Reference Lists**  
  - `atlas_country_info`  
  - `atlas_currency_info`  
- **Pricing**  
  - `atlas_price_book_daily`  
- **Sample Metering Feed**  
  - `atlas_meter_usage_daily`

These CSVs cover both *slowly changing reference data* (products, plans, countries, currencies) and *simulated dynamic feeds* (pricing and usage). This dual role is what makes Ledgerline portable across environments.

---

## Materialization and Lifecycle

Seeds are materialized as **Snowflake tables** in the `seeds` schema when you run `dbt seed`. A few important rules apply:

- **Typing**: Each CSV specifies `+column_types` so Snowflake doesn’t guess. This keeps the schema predictable.  
- **Lineage**: A post-hook sets `load_ts` if it’s null, giving every row a load timestamp even when the CSV didn’t include one.  
- **Performance**: Seeds are cheap to query. Because they only reload when the CSV changes, they are efficient even in large dev teams.  

**Environment-specific behavior**:

- In **dev/CI**, staging models read directly from these seed tables. The `atlas_meter` source also resolves to `_seeds`, so the full usage pipeline works without external dependencies.  
- In **prod**, ingestion schemas replace feeds like `atlas_meter_usage_daily` and `atlas_price_book_daily`. Catalogs and reference data may remain seeded for convenience, or migrate to mastered upstream sources.  

This design means **the same dbt code runs in all environments**, but the source of truth can change without breaking contracts.

---

## Contracts and Constraints

Seeds serve as the **first contract layer** of the pipeline. Each one declares basic uniqueness or combination tests in `seeds.yml`. This ensures that invalid shapes are caught early.

Key constraints:

- `atlas_crm_customer_info`: `customer_code` must be unique.  
- `atlas_catalog_product_info`: `product_code` must be unique.  
- `atlas_catalog_plan_info`: `plan_code` must be unique.  
- `atlas_currency_info`: `currency_code` must be unique.  
- `atlas_country_info`: `country_code` must be unique.  
- `atlas_price_book_daily`: unique combination `(product_code, plan_code, price_date)`.  
- `atlas_meter_usage_daily`: unique combination `(customer_code, product_code, plan_code, report_date)`.

These constraints **mimic reality** (customers must have one identity, usage must not double-count) and protect staging from broken inputs.

---

## Hashing and Keys

Seeds are stored as natural-key tables. **Hashing is deferred to staging**, where we introduce:

- **Stable surrogate keys** like `customer_hkey = hash(customer_code)`  
- **Version hashes** like `customer_hdiff = hash(customer_code, customer_name, country_code2)`

This design separates concerns:
- Seeds remain **clean, human-readable CSVs**.  
- Staging enforces **machine-stable keys and change detection**.  

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

This two-tier strategy means **cheap, broad tests at the seed stage** and **stricter, domain-aware tests in staging**.

---

## Operational Notes

- Keep seed CSVs **small, readable, and version-controlled**. Treat them like code: review diffs carefully.  
- If you rename or add a column, update its `+column_types` and any tests that reference it.  
- Timestamps and dates are always **UTC**. No conversions are done in seeds.  
- Seeds are contracts, not throwaways. They anchor the pipeline for local development and protect against unexpected upstream changes.  

---

## Why This Matters

Seeds strike a balance between agility and discipline. They give you **just enough structure** to simulate production data, validate assumptions, and develop downstream models with confidence. At the same time, the design makes it easy to **swap in real ingestion pipelines** later without rewriting staging or marts. This keeps Ledgerline both **portable for development** and **trustworthy for analytics**.
