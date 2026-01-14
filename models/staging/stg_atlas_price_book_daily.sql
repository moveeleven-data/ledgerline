/**
 * stg_atlas_price_book_daily.sql
 * ------------------------------
 * Staging model for the daily price book feed.
 *
 * Purpose:
 * - Normalize product_code and plan_code.
 * - Ensure price_date is valid and coerced into 21st century.
 * - Remove ghost rows (completely empty).
 * - Keep only the latest row per (product_code, plan_code, price_date).
 * - Add a default row for safe joins.
 * - Generate surrogate keys for uniqueness and change tracking.
 */

with

source_prices as (
    select
          upper(product_code)                       as product_code
        , upper(plan_code)                          as plan_code
        , {{ to_21st_century_date('price_date') }}  as price_date
        , unit_price                                as unit_price
        , to_timestamp_ntz(load_ts)                 as load_ts_utc
        , 'price_book'                              as record_source
    from {{ ref('atlas_price_book_daily') }}
)

, ghost_rows_removed as (
    select *
    from source_prices
    where (
          nullif(trim(product_code), '') is not null
      and nullif(trim(plan_code),   '')  is not null
      and price_date                     is not null
      and unit_price                     is not null
    )
)

, default_row as (
    select
          '-1'                           as product_code
        , '-1'                           as plan_code
        , to_date('2020-01-01')          as price_date
        , 0::number                      as unit_price
        , to_timestamp_ntz('2020-01-01') as load_ts_utc
        , 'System.DefaultKey'            as record_source
)

, combined_prices as (
    select
        *
    from ghost_rows_removed

    union all

    select
        *
    from default_row
)

, latest_prices as (
    select
        *
    from combined_prices

    qualify row_number() over (
        partition by
            product_code
          , plan_code
          , price_date
        order by
            load_ts_utc desc
    ) = 1
)

, hashed_prices as (
    select
          {{ dbt_utils.generate_surrogate_key(['product_code']) }} as product_hkey
        , {{ dbt_utils.generate_surrogate_key(['plan_code']) }}    as plan_hkey

        , {{ dbt_utils.generate_surrogate_key([
               'product_code'
             , 'plan_code'
             , 'price_date'
          ]) }} as price_book_hkey

        , {{ dbt_utils.generate_surrogate_key([
               'product_code'
             , 'plan_code'
             , 'price_date'
             , 'unit_price'
          ]) }} as price_book_hdiff
        
        , *
    from latest_prices
)

select
    *
from hashed_prices
