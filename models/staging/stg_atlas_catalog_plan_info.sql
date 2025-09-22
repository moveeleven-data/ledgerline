{{ config(materialized='ephemeral') }}

with

src as (
    select
          upper(plan_code)                  as plan_code
        , plan_name
        , upper(product_code)               as product_code
        , billing_period
        , to_timestamp_ntz(load_ts)         as load_ts
        , 'SEED.atlas_catalog_plan_info'    as record_source
    from {{ ref('plans') }}
)

, default_row as (
    select
          '-1'                           as plan_code
        , 'Missing'                      as plan_name
        , '-1'                           as product_code
        , 'unknown'                      as billing_period
        , to_timestamp_ntz('2020-01-01') as load_ts
        , 'System.DefaultKey'            as record_source
)

, unioned as (
    select * from src
    union all
    select * from default_row
)

, hashed as (
    select
          {{ dbt_utils.generate_surrogate_key(['plan_code']) }} as plan_hkey
        , {{ dbt_utils.generate_surrogate_key([
               'plan_code'
              ,'plan_name'
              ,'product_code'
              ,'billing_period'
          ]) }} as plan_hdiff

        , * exclude (load_ts)
        , load_ts as load_ts_utc
    from unioned
)

select * from hashed
