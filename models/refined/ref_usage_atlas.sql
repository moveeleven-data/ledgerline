/**
 * ref_usage_atlas.sql
 * -------------------------------
 * Refined daily usage feed.
 *
 * Purpose:
 * - Collapse history to the latest valid OPEN row per usage_hkey.
 * - Compute overage_units for convenience (units_used - included_units).
 * - Ensure only clean, deduped rows flow into pricing and facts.
 *
 * Grain:
 * - One row per usage_hkey (latest OPEN record).
 */

select
      usage_hkey      as usage_key
    , usage_hdiff     as usage_diff_key
    , customer_hkey   as customer_key
    , product_hkey    as product_key
    , plan_hkey       as plan_key
    , customer_code
    , product_code
    , plan_code
    , report_date
    , units_used
    , included_units
    , units_used - included_units as overage_units
    , record_source
    , load_ts_utc
from {{ ref('stg_atlas_meter_usage_daily') }}
