{{ config(materialized='ephemeral') }}

with

src as (
    select
          upper(product_code)                       as product_code
        , upper(plan_code)                          as plan_code
        , {{ to_21st_century_date('price_date') }}  as price_date
        , coalesce(unit_price, 0)                   as unit_price
        , to_timestamp_ntz(load_ts)                 as load_ts
        , 'SEED.atlas_price_book_daily'             as record_source
    from {{ ref('price_book_daily') }}
)

, default_row as (
    select
          '-1'                           as product_code
        , '-1'                           as plan_code
        , to_date('2020-01-01')          as price_date
        , 0::number                      as unit_price
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
          {{ dbt_utils.generate_surrogate_key([
                'product_code'
               ,'plan_code'
               ,'price_date'
          ]) }} as price_book_hkey

        , {{ dbt_utils.generate_surrogate_key([
                'product_code'
               ,'plan_code'
               ,'price_date'
               ,'unit_price'
          ]) }} as price_book_hdiff

        , * exclude (load_ts)
        , load_ts as load_ts_utc
    from unioned
)

select * from hashed
