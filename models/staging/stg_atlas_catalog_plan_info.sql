/**
 * stg_atlas_catalog_plan_info.sql
 * -------------------------------
 * Staging model for subscription plan catalog data.
 *
 * Purpose:
 * - Normalize plan_code and product_code.
 * - Add a default row for safe joins.
 * - Generate surrogate keys for uniqueness and change tracking.
 */

with

plan_source as (
    select
          upper(plan_code)                  as plan_code
        , plan_name
        , upper(product_code)               as product_code
        , billing_period
        , to_timestamp_ntz(load_ts)         as load_ts
        , 'SEED.atlas_catalog_plan_info'    as record_source
    from {{ ref('atlas_catalog_plan_info') }}
)

, plan_default_row as (
    select
          '-1'                           as plan_code
        , 'Missing'                      as plan_name
        , '-1'                           as product_code
        , 'unknown'                      as billing_period
        , to_timestamp_ntz('2020-01-01') as load_ts
        , 'System.DefaultKey'            as record_source
)

, plan_combined as (
    select
        *
    from plan_source

    union all

    select
        *
    from plan_default_row
)

, plan_hashed as (
    select
          {{ dbt_utils.generate_surrogate_key([
               'plan_code'
          ]) }} as plan_hkey

        , {{ dbt_utils.generate_surrogate_key([
               'plan_code'
              ,'plan_name'
              ,'product_code'
              ,'billing_period'
          ]) }} as plan_hdiff

        , * exclude (load_ts)
        , to_timestamp_ntz('{{ run_started_at }}') as load_ts_utc
    from plan_combined
)

select
    *
from plan_hashed
