/**
 * stg_atlas_catalog_plan_info.sql
 * -------------------------------
 * Staging model for subscription plan catalog data.
 *
 * Purpose:
 * - Normalize plan_code and product_code.
 * - Add a default row for safe joins.
 * - Generate surrogate keys for uniqueness and change tracking.
 * - Keep only the latest version per plan_code by load_ts_utc.
 */

with

plan_source as (
    select
          upper(plan_code)               as plan_code
        , plan_name
        , upper(product_code)            as product_code
        , billing_period
        , to_timestamp_ntz(load_ts)      as load_ts_utc
        , 'SEED.atlas_catalog_plan_info' as record_source
    from {{ ref('atlas_catalog_plan_info') }}
)

, plan_latest as (
    select
        *
    from plan_source

    qualify row_number() over (
        partition by
            plan_code
        order by
            load_ts_utc desc
    ) = 1
)

, plan_hashed as (
    select
          {{ dbt_utils.generate_surrogate_key(['plan_code']) }} as plan_hkey

        , plan_code
        , plan_name
        , product_code
        , lower(billing_period) as billing_period
        , load_ts_utc
        , record_source
    from plan_latest
)

select
    *
from plan_hashed
