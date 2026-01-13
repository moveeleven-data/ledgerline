/**
 * ref_plan_atlas.sql
 * -------------------------------
 * Plan dimension for the star schema.
 *
 * Purpose:
 * - Provide a clean set of plan attributes (code, name, product, billing period) for downstream marts.
 * - Alias the surrogate key from staging (`plan_hkey`) as `plan_key` to align with star-schema naming.
 *
 * Grain:
 * - One row per plan_key (derived from the staging surrogate key).
 */

select
      plan_hkey as plan_key
    , plan_code
    , plan_name
    , product_code
    , billing_period
    , record_source
    , load_ts_utc
from {{ ref('stg_atlas_catalog_plan_info') }}
