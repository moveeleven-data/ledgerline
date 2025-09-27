# Source Systems

Sources are declarations of external tables that dbt does not create. They define where data lands in the warehouse,
how fresh it should be, and what its contract is. 

In Ledgerline, the only true source is the Atlas metering feed. This feed records how much each customer used each product and plan on a given day. It appends new rows daily.

We track freshness, enforce uniqueness at the daily grain, and apply staged cleaning logic. 

The other inputs - customer, product, plan, currency, and country - are modeled as seeds since they are relatively static.  

*(The daily price book is seeded as well for portability. In a production system, it would be treated as a source with freshness tests, since rates change over time.)*

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

## Atlas Metering Source

**Logical name**: `atlas_meter`  
**Table**: `atlas_meter_usage_daily`

**Database resolution**:  
- Uses `{{ target.database }}` so Dev/QA/Prod each hit their own database (e.g., `LEDGER_LINE_DEV`, `LEDGER_LINE_QA`, `LEDGER_LINE_PROD`).

**Schema resolution**:  
- **Default (Dev/QA/CI)** → `{{ generate_schema_name('SEEDS', this) }}` (e.g., `DBT_MTRIPODI_SEEDS`). We build the seeds first, so sources read from seeded tables for reproducible runs.  
- **Production override** → Set the env var `DBT_ATLAS_METER_SCHEMA` to your raw ingestion schema (e.g., `SOURCE_DATA`) so production reads live landed data.

This keeps one code path: seeds in Dev/QA, raw in Prod (via env var), with the database set per environment.

---

## Freshness and Quality Checks

We check the Atlas metering feed as soon as it lands in the warehouse. We make sure the data is recent and shaped correctly before it flows further downstream.  

- **Freshness SLA**  
  - Warn if more than **36 hours** old  
  - Error if more than **72 hours** old

- **Grain contract**  
  - Exactly one row per `(customer_code, product_code, plan_code, report_date)`  
  - No duplicates allowed  

If the data is stale or doesn’t meet the expected grain, the tests stop the process. This ensures that staging, history, and marts all build on top of healthy input data.

---

## Testing Strategy

Source-level tests protect **upstream integrity**

- `dbt_utils.unique_combination_of_columns` on the declared grain.  
- Freshness checks with explicit warn/error windows.  
- Column presence, types, and descriptions to lock the contract.  

Anything more sophisticated (like nonnegativity, sanity checks, or deduplication) is handled in staging.