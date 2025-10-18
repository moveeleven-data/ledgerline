# History Layer

The **History layer** is Ledgerline’s durable record of change. It stores *what arrived and when* so we can trace data over time. Staging cleans inputs; History keeps them, with timestamps, for audit and replay.

---

## What we store (now)

- **Usage history**: *append-only* daily snapshots. Each run writes the rows for a single `report_date` from staging—no synthetic CLOSE rows, no merges.  
  **Grain:** one row per `(customer_code, product_code, plan_code, report_date)` per load.  
  **Keys:**  
  - `usage_hkey = hash(customer_code, product_code, plan_code, report_date)`  
  - `usage_hdiff = hash(customer_code, product_code, plan_code, report_date, units_used, included_units)`

- **Reference history** (customers, products, plans, countries, currencies): SCD-style append using `save_history`—each new version appends a row; Refined later collapses to the latest.

---

## Materialization

- **Usage**: `incremental` with **append** (no merge). We keep a ledger of what was received on each day.  
- **Reference**: `incremental` with **append** via `save_history`.  
- `on_schema_change: ignore` to avoid rewriting old partitions.

> Note: Because usage is append-only, re-running the same **report_date** would add another copy of that day’s rows. If you need idempotent reruns for a day, delete that day’s slice first or switch the model to a merge strategy.

---

## Tests we rely on

- Not-null on keys and dates.  
- Uniqueness at the intended grain (e.g., `(usage_hkey, report_date)` for usage).  
- Hash collision checks on `usage_hkey` and `usage_hdiff`.

These keep the ledger consistent without adding business logic here.

---

## How downstream uses it

- **Refined** picks the **latest** row per key (or key+date) when a current view is needed.  
- **Marts** apply business logic (e.g., pricing) on top of those current views.

History is the memory: append snapshots for usage, append versions for references—simple, auditable, and faithful to what actually arrived.

---

### Why there’s no separate history table for the Price Book

The price book is already a daily record. Each row shows the price for a product and plan on a specific date.  

`stg_atlas_price_book_daily` keeps the latest version for each day, and `ref_price_book_daily` adds currency and supports joins in usage reports.  

A separate history table would only be useful if prices for the same day could change later.
