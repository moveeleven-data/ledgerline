/**
 * dim_plan.sql
 * ------------
 * Pass-through of the staging plan catalog.
 * Grain: one row per plan_key.
 */

select
      plan_hkey as plan_key
    , plan_code
    , plan_name
    , product_code
    , billing_period
from {{ ref('stg_atlas_catalog_plan_info') }}
