/**
 * ref_usage_atlas.sql
 * -------------------------------
 * Refined daily usage feed.
 *
 * Purpose:
 * - Publish a stable usage interface for marts with consistent `*_key` naming.
 * - Compute overage_units for convenience (units_used - included_units).
 * - Rely on staging for deduplication and ingestion correctness (no re-implementation here).
 *
 * Grain:
 * - One row per customer_key × product_key × plan_key × report_date.
 */

select
      usage_hkey      as usage_key
    , customer_hkey   as customer_key
    , product_hkey    as product_key
    , plan_hkey       as plan_key
    , customer_code
    , product_code
    , plan_code
    , report_date
    , units_used
    , included_units
    , greatest(units_used - included_units, 0) as overage_units
    , record_source
    , load_ts_utc
from {{ ref('stg_atlas_meter_usage_daily') }}
