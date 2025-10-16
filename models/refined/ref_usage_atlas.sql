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

with latest_per_key as (
    select
        *
    from {{ ref('hist_atlas_meter_usage_daily') }}

    qualify row_number() over (
        partition by
             usage_hkey
        order by
             report_date desc
           , ingestion_ts desc
    ) = 1
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
    , ingestion_ts
from latest_per_key