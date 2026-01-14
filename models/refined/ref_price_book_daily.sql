/**
 * ref_price_book_daily.sql
 * ------------------------
 * REF wrapper for the daily price book (adds currency + key).
 */

with

price_book_source as (
    select
          product_hkey
        , plan_hkey
        , product_code
        , plan_code
        , price_date
        , unit_price
        , record_source
        , load_ts_utc
    from {{ ref('stg_atlas_price_book_daily') }}
)

select
      product_hkey as product_key
    , plan_hkey    as plan_key
    , product_code
    , plan_code
    , price_date
    , unit_price
    , record_source
    , load_ts_utc
from price_book_source