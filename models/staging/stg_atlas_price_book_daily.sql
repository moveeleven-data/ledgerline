/**
 * stg_atlas_price_book_daily.sql
 * ------------------------------
 * Staging model for the daily price book feed.
 *
 * Purpose:
 * - Normalize product_code and plan_code.
 * - Ensure price_date is valid and coerced into 21st century.
 * - Enforce numeric precision for unit_price.
 * - Keep only the latest row per (product_code, plan_code, price_date).
 * - Generate surrogate keys for uniqueness.
 */

with

source_prices as (
    select
          upper(product_code)                            as product_code
        , upper(plan_code)                               as plan_code
        , {{ to_21st_century_date('price_date') }}::date as price_date

        , try_to_number(unit_price)::number(18,6)        as unit_price

        , to_timestamp_ntz(load_ts)                      as load_ts_utc
        , 'SEED.atlas_price_book_daily'                  as record_source
    from {{ ref('atlas_price_book_daily') }}
)

, latest_prices as (
    select
        *
    from source_prices

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

        , *
    from latest_prices
)

select
    *
from hashed_prices
