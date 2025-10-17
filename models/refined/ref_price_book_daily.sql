/**
 * ref_price_book_daily.sql
 * ------------------------
 * REF wrapper for the daily price book.
 */
select
      upper(product_code)           as product_code
    , upper(plan_code)              as plan_code
    , price_date
    , unit_price
    , record_source
    , ingestion_ts
from {{ ref('stg_atlas_price_book_daily') }}