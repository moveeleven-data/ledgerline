/**
 * ref_plan_atlas.sql
 * -------------------------------
 * Refined dimension for subscription plans.
 *
 * Purpose:
 * - Collapse SCD history to the current plan record per surrogate key.
 * - Provide stable attributes (plan name, product, billing period) for marts.
 *
 * Grain:
 * - One row per plan_hkey.
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
