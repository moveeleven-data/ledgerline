# History Layer

The **History layer** stores *what arrived and when* so changes can be traced over time. Staging cleans inputs; History keeps the raw arrivals with timestamps for audit and reproducibility.

## What we store

- **Usage history**: daily usage arrivals kept in `hist_atlas_meter_usage_daily` via incremental **MERGE** keyed by `(usage_hkey, report_date)` (idempotent reruns for the same day).  
  **Grain:** one row per `usage_hkey × report_date`.  
  **Keys:**
  - `usage_hkey = hash(customer_code, product_code, plan_code, report_date)`
  - `usage_hdiff = hash(customer_code, product_code, plan_code, report_date, units_used, included_units)`

Static reference history (customers, products, plans, countries, currencies) is no longer stored here.

## Materialization

- `hist_atlas_meter_usage_daily`: incremental MERGE on `(usage_hkey, report_date)` to support reloading a specific `report_date` without duplicating rows.

## Tests we rely on

- `not_null` on: `usage_hkey`, `usage_hdiff`, `report_date`, `load_ts_utc`
- Uniqueness at the intended grain: `(usage_hkey, report_date)`
- Hash collision checks for `usage_hkey` and `usage_hdiff`

## How downstream uses it

- **Refined** selects the latest available record per `usage_hkey` (by `report_date` and `load_ts_utc`) when a “current” usage view is needed.
- **Marts** apply business logic like price joins and value metrics on top of refined usage.

## Price book note

The price book is modeled as a daily feed in staging and a refined wrapper (`ref_price_book_daily`) that standardizes codes and adds currency keys. A separate history table would only be needed if same-day prices were expected to be revised later and you needed to retain prior versions.