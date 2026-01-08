# History Layer

The **History layer** is Ledgerline’s durable record of change. It stores *what arrived and when* so we can trace data over time. Staging cleans inputs; History keeps them, with timestamps, for audit and replay.

---

## What we store (now)

- **Usage history**: daily snapshots written via **MERGE** (idempotent). Each run processes one `report_date` from staging—no synthetic CLOSE rows.  
  **Grain:** one row per `(customer_code, product_code, plan_code, report_date)`.  
  **Keys:**  
  - `usage_hkey = hash(customer_code, product_code, plan_code, report_date)`  
  - `usage_hdiff = hash(customer_code, product_code, plan_code, report_date, units_used, included_units)`

- **Reference history** (customers, products, plans, countries, currencies): SCD-style append using `save_history`—each new version appends a row; Refined later collapses to the latest.

---

## Materialization

- Usage history: incremental MERGE keyed by (usage_hkey, report_date) to support idempotent reruns. We keep one row per customer/product/plan per day, and rerunning the same day updates that snapshot rather than duplicating it.
- Reference history: SCD-style append via save_history, and Refined selects the latest version per key.

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

Lederline's history layer merges daily usage snapshots and append new reference versions, so downstream models can rely on stable keys and reproducible ‘as-of’ views.

---

### Why there’s no separate history table for the Price Book

The price book is already a daily record. Each row shows the price for a product and plan on a specific date.  

`stg_atlas_price_book_daily` keeps the latest version for each day, and `ref_price_book_daily` adds currency and supports joins in usage reports.  

A separate history table would only be useful if prices for the same day could change later.

