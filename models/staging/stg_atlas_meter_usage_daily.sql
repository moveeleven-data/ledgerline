{{ config(materialized='ephemeral') }}

with

src as (
    select
          upper(customer_code)                       as customer_code
        , upper(product_code)                        as product_code
        , upper(plan_code)                           as plan_code
        , {{ to_21st_century_date('report_date') }}  as report_date
        , coalesce(units_used, 0)                    as units_used
        , coalesce(included_units, 0)                as included_units
        , to_timestamp_ntz(load_ts)                  as load_ts
        , 'SEED.atlas_meter_usage_daily'             as record_source
    from {{ ref('atlas_meter_usage_daily') }}
)

, dedup as (
    select *
    from src
    qualify row_number() over (
        partition by
            customer_code
          , product_code
          , plan_code
          , report_date
        order by
            load_ts desc
          , units_used desc
    ) = 1
)

, default_row as (
    select
          '-1'                           as customer_code
        , '-1'                           as product_code
        , '-1'                           as plan_code
        , to_date('2020-01-01')          as report_date
        , 0::number                      as units_used
        , 0::number                      as included_units
        , to_timestamp_ntz('2020-01-01') as load_ts
        , 'System.DefaultKey'            as record_source
)

, unioned as (
    select * from dedup
    union all
    select * from default_row
)

, hashed as (
    select
        {{ dbt_utils.generate_surrogate_key([
              'customer_code'
              ,'product_code'
              ,'plan_code'
              ,'report_date'
        ]) }} as usage_hkey

        , {{ dbt_utils.generate_surrogate_key([
              'customer_code'
            , 'product_code'
            , 'plan_code'
            , "to_varchar(report_date,'YYYY-MM-DD')"
            , 'units_used','included_units'
          ]) }} as usage_hdiff

        , * exclude (load_ts)
        , to_timestamp_ntz('{{ run_started_at }}') as load_ts_utc
    from unioned
)

select * from hashed
