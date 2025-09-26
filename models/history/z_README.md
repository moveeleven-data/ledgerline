# History Layer

History is the durable change log of the Ledgerline pipeline. It preserves *what was true and when it was true*.

Where staging adapts and cleans raw inputs, history makes them persistent, auditable, and time-aware.

This layer has two main patterns:

- **Reference entities**: Slowly changing dimensions captured with `save_history` (SCD Type 2).  
- **Usage spans**: Daily incremental merge that inserts `OPEN` rows and generates `CLOSE_SYNTHETIC` rows when a previously open key disappears.

---

## Usage History

**Grain**: one row per Business Key × Date × Row Type.

### Row types
- `OPEN`: A row that arrived in the feed for the processing date.  
- `CLOSE_SYNTHETIC`: A zero row generated when yesterday had an open key that is absent today. This makes churn explicit and auditable.

### Surrogate and versioning
- `usage_hkey = hash(customer_code, product_code, plan_code, report_date)`  
- `usage_hdiff = hash(customer_code, product_code, plan_code, report_date, units_used, included_units)`  

Synthetic closes reuse the same `usage_hkey` for the day, but generate a different `usage_hdiff` by overriding usage values to zero.

This guarantees uniqueness between the last OPEN and the CLOSE row.

### Daily Processing Logic

1. Determine the usage date being processed. This is normally the latest available date, but you can override it with a variable when doing backfills. (*`get_latest_usage_report_date`*)  

2. Pull in today’s rows from staging and mark them as `OPEN`. These represent the actual usage observed for that day.  

3. Look back to find the most recent `OPEN` row for each business key before the processing date. This helps identify subscriptions that were active yesterday. (*`latest_prior_open`*)  

4. For any key that was active yesterday but is missing today, create a synthetic `CLOSE` row. This explicitly records churn by inserting a zero row for the missing subscription. (*`synthetic_close`*)  

5. Deduplicate results so that for each key, date, and row type, you only keep the latest row by load timestamp and version hash.  

6. Write the changes into the history table using an incremental merge, keyed by surrogate ID, date, and row type. This ensures idempotent updates without rewriting past data.

---

## Reference History

Reference entities (customers, products, plans, countries, currencies) use the **generic macro `save_history`**:

- **First run**: Append all rows from staging.  
- **Incremental runs**: Append only rows with new `*_hdiff` values.  
- **Refined layer**: Use `current_from_history` to collapse to the latest row per key.  

This gives each reference table a stable surrogate key and a time-aware record of attribute changes.

---

## Materialization and Schema Drift

All history models are **incremental**.  
- **Reference entities** use append, since new versions only add rows.  
- **Usage history** uses merge, so each day’s OPEN rows can pair with synthetic CLOSE rows without leaving duplicates.  
- `on_schema_change: ignore` keeps history stable if staging evolves. New columns can be added later without rewriting old data.

---

### Why Incremental + Merge

Usage history is materialized as **incremental with `merge`** because of how the data behaves. Each day you need to insert `OPEN` rows for the current date and potentially add `CLOSE` rows for keys that disappeared. This means the active day’s slice can change, but past days must remain untouched. A merge lets us upsert rows at the `(business key × date × row type)` grain without rewriting the entire table. It is efficient, idempotent, and preserves a clean audit trail.

Other materialization strategies don’t fit as well:

- **Table** would rebuild the full history every run. This is wasteful at scale.
- **View** would recompute history logic (synthetic closes, dedupes) every query. That keeps storage cheap but pushes heavy compute to consumers.
- **Ephemeral** means that history would not exist as a durable artifact at all. You would lose the ability to audit changes over time.  
- **Incremental (append)** works well for reference dimensions, because new versions simply append when attributes change. But append cannot handle usage, because a key may need to switch from `OPEN` to `CLOSE`, which would leave duplicates or stale rows.
- **Incremental (delete+insert)** enforces correctness by rewriting partitions, but it is heavy-handed and inefficient compared to merge.

By contrast, **incremental + merge** only rewrites the active day’s rows, keeps older partitions frozen. This design balances cost, auditability, and correctness.

---

## Testing Strategy

History tests focus on making sure the change log is stable and correct.  

- Keys and dates must always be present.  
- Row types are limited to `OPEN` and `CLOSE_SYNTHETIC`.  
- Each key, date, and row type combination should appear only once.  
- Hashes are checked to prevent collisions.  

We also use continuity checks to confirm that if a key was open yesterday, it still shows up today (either still open or properly closed).

---

## Operational Notes

- To reprocess a specific day, run with `--vars "as_of_date: YYYY-MM-DD"` (UTC date). This rebuilds that day’s `OPEN` and `CLOSE_SYNTHETIC` rows.  

>> Word of caution. Changing a single day in isolation may alter the baseline that later days depend on. If continuity matters (for example, churn counts), re-run the subsequent days as well.

- Reference tables are append-only. Each new attribute version simply appends a row without altering prior records.

- Usage history evolves one day at a time through incremental merge. The merge key makes reruns of the same date idempotent, but downstream continuity is only guaranteed if you process days in sequence.

---

## Why This Layer Matters

Without history, the pipeline would only hold the latest snapshot. History makes time explicit, supporting:

- Auditable churn and closes  
- Reproducible backfills  
- Slowly changing dimension attributes  
- Stable surrogate keys  

It is the memory of the system, ensuring Ledgerline analytics can answer not just what is true now, but what was true at any point in time.
