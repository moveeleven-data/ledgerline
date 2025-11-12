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

with latest_any as (
    select
        *
    from {{ ref('hist_atlas_meter_usage_daily') }}

    qualify row_number() over (
        partition by
            usage_hkey
        order by
            report_date desc
          , load_ts_utc desc
    ) = 1
)

, latest_open as (
    select
        *
    from latest_any
)

select
      usage_hkey
    , usage_hdiff
    , customer_hkey
    , product_hkey
    , plan_hkey
    , customer_code
    , product_code
    , plan_code
    , report_date
    , units_used
    , included_units
    , record_source
    , load_ts_utc
from latest_open