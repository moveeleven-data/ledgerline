# History Layer

History is the **durable change log** of the Ledgerline pipeline. It preserves *what was true and when it was true*. Where staging adapts and cleans raw inputs, history makes them **persistent, auditable, and time-aware**.

This layer has two main patterns:

- **Reference entities**: Slowly changing dimensions captured with `save_history` (SCD Type 2).  
- **Usage spans**: Daily incremental merge that inserts `OPEN` rows and generates `CLOSE_SYNTHETIC` rows when a previously open key disappears.

---

## Usage History

**Grain**: one row per `usage_hkey` × `report_date` × `usage_row_type`.

### Row types
- **OPEN** — A row that arrived in the feed for the processing date.  
- **CLOSE_SYNTHETIC** — A zero row generated when yesterday had an open key that is absent today. This makes churn explicit and auditable.

### Surrogate and versioning
- `usage_hkey = hash(customer_code, product_code, plan_code, report_date)`  
- `usage_hdiff = hash(customer_code, product_code, plan_code, report_date, units_used, included_units)`  

Synthetic closes **reuse the same `usage_hkey`** for the day, but generate a different `usage_hdiff` by overriding usage values to zero. This guarantees uniqueness between the last OPEN and the CLOSE row.

### Daily processing logic
1. `get_latest_usage_report_date` sets the **as-of date** (can be overridden with a var for backfills).  
2. Pull today’s `OPEN` rows from staging.  
3. `latest_prior_open` finds the most recent open row per business key before the as-of date.  
4. `synthetic_close` emits one close row per prior key missing today.  
5. Deduplicate on `(usage_hkey, report_date, usage_row_type)` keeping the latest by `load_ts_utc` and `usage_hdiff`.  
6. Incrementally `merge` into the history table with `unique_key = [usage_hkey, report_date, usage_row_type]`.

### Why usage uses merge
- Only the active day’s rows are rewritten.  
- Backfills are reproducible by running with `--vars "as_of_date: YYYY-MM-DD"`.  
- Churn and carry forward are **explicit**: closes are stored, not inferred.  

---

## Reference History

Reference entities (customers, products, plans, countries, currencies) use the **generic macro `save_history`**:

- **First run**: append all rows from staging.  
- **Incremental runs**: append only rows with new `*_hdiff` values.  
- **Refined layer**: use `current_from_history` to collapse to the latest row per key.  

This gives each reference table a stable surrogate key and a time-aware record of attribute changes.

---

## Materialization and Schema Drift

- All history models are `incremental`.  
- Reference entities → `append` strategy (fast, simple, collision-free with version hash).  
- Usage history → `merge` strategy (necessary for daily OPEN/CLOSE upserts).  
- `on_schema_change: ignore` keeps history stable if staging evolves. New columns can be added deliberately later, without rewriting old partitions.  

---

## Hashing Strategy

- **Surrogate keys**: provide stable identity over natural keys. Examples:  
  - `customer_hkey = hash(customer_code)`  
  - `usage_hkey = hash(customer_code, product_code, plan_code, report_date)`  
- **Version hashes**: detect content changes. Examples:  
  - `customer_hdiff = hash(customer_code, customer_name, country_code2)`  
  - `usage_hdiff = hash(customer_code, product_code, plan_code, report_date, units_used, included_units)`  

By separating *identity* from *content*, history ensures stable surrogate keys while still tracking attribute changes.

---

## Testing Strategy

Tests are declared in `history.yml` and include:

- `not_null` on surrogate keys and date fields  
- `accepted_values` on `usage_row_type` (`OPEN`, `CLOSE_SYNTHETIC`)  
- `dbt_utils.unique_combination_of_columns` on `(usage_hkey, report_date, usage_row_type)`  
- Custom `hash_collision_free` tests on both `usage_hkey` and `usage_hdiff`  

Singular tests also check continuity across the as-of date, for example ensuring that any key open yesterday appears today as either still `OPEN` or newly `CLOSE_SYNTHETIC`.

---

## Time and Timezone

- `report_date` is always a **UTC calendar date**.  
- All `load_ts` fields are UTC.  
- No timezone conversions are performed in history.  

This keeps billing and churn logic simple and globally consistent.

---

## Operational Notes

- To reprocess a day: run with `--vars "as_of_date: YYYY-MM-DD"`. The merge key ensures idempotence.  
- For backfills: process days in order (oldest to newest) so synthetic closes behave correctly.  
- Reference history grows append-only; usage history evolves one day at a time.  

---

## Why This Layer Matters

Without history, the pipeline would only hold the latest snapshot. History makes **time explicit**, supporting:

- Auditable churn and closes  
- Reproducible backfills  
- Slowly changing dimension attributes  
- Stable surrogate keys  

It is the **memory of the system**, ensuring Ledgerline analytics can answer *not just what is true now, but what was true at any point in time*.
