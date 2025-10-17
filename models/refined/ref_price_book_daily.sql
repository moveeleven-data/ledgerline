/**
 * ref_price_book_daily.sql
 * ------------------------
 * REF wrapper for the daily price book (adds currency + key).
 */

with

price_book_source as (
    select
          upper(product_code) as product_code
        , upper(plan_code) as plan_code
        , price_date
        , unit_price
        , record_source
        , load_ts_utc
        , '{{ var("default_billing_currency", "USD") }}'::string as currency_code
    from {{ ref('stg_atlas_price_book_daily') }}
)

select
      product_code
    , plan_code
    , price_date
    , unit_price
    , currency_code
    , {{ dbt_utils.generate_surrogate_key(['currency_code']) }} as currency_key
    , record_source
    , load_ts_utc
from price_book_source