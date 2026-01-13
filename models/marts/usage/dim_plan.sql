/**
 * dim_plan.sql
 * ------------
 * Pass-through of the refined plan dimension.
 * Grain: one row per plan_key.
 */

select
      plan_key
    , plan_code
    , plan_name
    , product_code
    , billing_period
from {{ ref('ref_plan_atlas') }}
