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

with

current_from_history as (
    {{ current_from_history(
          history_relation      = ref('hist_atlas_catalog_plan_info')
        , key_column            = 'plan_hkey'
        , load_timestamp_column = 'ingestion_ts'
    ) }}
)

select
      plan_hkey
    , plan_code
    , plan_name
    , product_code
    , billing_period
    , record_source
    , ingestion_ts
from current_from_history
