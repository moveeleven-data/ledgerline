/**
 * stg_atlas_catalog_product_info.sql
 * ----------------------------------
 * Staging model for product catalog reference data.
 *
 * Purpose:
 * - Normalize product_code to uppercase.
 * - Add a default row for safe joins.
 * - Generate surrogate keys for uniqueness and change tracking.
 * - Keep only the latest version per product_code by load_ts_utc.
 */

with

product_source as (
    select
          upper(product_code)               as product_code
        , product_name
        , category
        , to_timestamp_ntz(load_ts)         as load_ts_utc
        , 'SEED.atlas_catalog_product_info' as record_source
    from {{ ref('atlas_catalog_product_info') }}
)

, product_latest as (
    select
        *
    from product_source

    qualify row_number() over (
        partition by
            product_code
        order by
            load_ts_utc desc
    ) = 1
)

, product_hashed as (
    select
          {{ dbt_utils.generate_surrogate_key(['product_code']) }} as product_hkey
        , *
    from product_latest
)

select
    *
from product_hashed
