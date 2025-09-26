# Ledgerline Sources

Sources declare **external tables that dbt does not create**. They define where data lands in the warehouse, how fresh it should be, and what its contract is. In Ledgerline, the primary runtime source is the **Atlas metering feed**. Other inputs (CRM, catalog, reference, pricing) are represented as seeds in development, but could later be promoted to true sources if upstream systems land them.

---

## Atlas Metering Source

- **Logical name**: `atlas_meter`  
- **Table**: `atlas_meter_usage_daily`  
- **Schema resolution**:  
  - **Dev and CI**: `{{ target.schema }}_seeds` — resolves to the seeded usage table so the full pipeline is reproducible without upstream feeds.  
  - **Prod**: `{{ env_var("DBT_ATLAS_METER_SCHEMA", target.schema ~ "_seeds") }}` — resolves to the true raw landing schema.  

This indirection allows the **same dbt code to run in all environments**, with only the schema locator changing.

---

## Freshness and Quality Gates

The source enforces **governance checks at the warehouse edge**:

- `loaded_at_field = load_ts` is the freshness indicator.  
- **Freshness SLA**:  
  - Warn if more than 36 hours old  
  - Error if more than 72 hours old  
- **Grain contract**: one row per `(customer_code, product_code, plan_code, report_date)`.  

These rules live in `source_atlas_yml` and fail fast when the feed is stale or duplicated, preventing bad data from propagating into history and marts.

---

## Flow into Staging

- **Usage feed**: staging models read directly from `source('atlas_meter','atlas_meter_usage_daily')`.  
- **CRM, catalog, reference, pricing**: in development, staging reads from seeded tables (`ref(...)`). In production, these could be upgraded to real sources if ingestion pipelines land them upstream.  

This layered approach lets you start lightweight with seeds, but **graduate to full source contracts** without rewriting downstream code.

---

## Design Principles

- **Grain enforced at source**: Uniqueness tests make sure the raw feed respects its daily grain. Staging will still dedupe, but the first line of defense is here.  
- **UTC only**: `report_date` is always the calendar day in UTC. No time zone conversions are done in this layer.  
- **Freshness is a gate**: If the feed is late, downstream jobs should block or run in safe mode.  

---

## Hashing Strategy

There is **no hashing at the source level**. Sources expose the natural keys and attributes exactly as landed. Hashes and surrogate keys are introduced in staging, where deterministic field lists guarantee stable key generation across environments.

---

## Testing Strategy

Source-level tests focus on **upstream integrity**:

- `dbt_utils.unique_combination_of_columns` to enforce the natural grain.  
- Freshness checks with explicit warn and error windows.  
- Column presence and descriptions to lock the contract.  

More sophisticated validations (nonnegativity, year sanity checks, deduplication) happen in staging.

---

## Documentation Posture

Column descriptions in the source YAML define the **public contract** of the upstream system. Downstream models rely on these definitions to maintain consistency. If an upstream team renames or repurposes a column, source tests will surface the break before it contaminates history or marts.

---

## Operational Notes

- Treat `source()` as the **single point of contract** with upstream. Any change to raw schemas should be absorbed here, not hacked around in marts.  
- Remember: sources are **declarations only**. dbt does not create them, it only references them.  
- In dev and CI, seeds give you a safe, reproducible sandbox. In prod, sources give you SLAs and accountability.  
