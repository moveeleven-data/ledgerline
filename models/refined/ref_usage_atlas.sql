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
        partition by usage_hkey
        order by
            report_date desc
          , load_ts_utc desc
    ) = 1
)

, latest_open as (
    select
        *
    from latest_any
    where
          usage_row_type   = 'OPEN'
      and report_date      is not null
      and units_used       is not null
      and included_units   is not null
)

select
    usage_hkey
  , customer_code
  , product_code
  , plan_code
  , record_source
  , report_date
  , units_used
  , included_units
  , greatest(units_used - included_units, 0) as overage_units
  , load_ts_utc
  , usage_row_type
  
from latest_open