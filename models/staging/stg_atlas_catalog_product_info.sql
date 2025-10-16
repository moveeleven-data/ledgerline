/**
 * stg_atlas_catalog_product_info.sql
 * ----------------------------------
 * Staging model for product catalog reference data.
 *
 * Purpose:
 * - Normalize product_code to uppercase.
 * - Add a default row for safe joins.
 * - Generate surrogate keys for uniqueness and change tracking.
 * - Keep only the latest version per product_code by ingestion_ts.
 */

with

product_source as (
    select
          upper(product_code)               as product_code
        , product_name
        , category
        , to_timestamp_ntz(load_ts)         as ingestion_ts
        , 'SEED.atlas_catalog_product_info' as record_source
    from {{ ref('atlas_catalog_product_info') }}
)

, product_default_row as (
    select
          '-1'                           as product_code
        , 'Missing'                      as product_name
        , 'Missing'                      as category
        , to_timestamp_ntz('2020-01-01') as ingestion_ts
        , 'System.DefaultKey'            as record_source
)

, product_combined as (
    select
        *
    from product_source

    union all

    select
        *
    from product_default_row
)

, product_latest as (
    select
        *
    from product_combined

    qualify row_number() over (
        partition by
            product_code
        order by
            ingestion_ts desc
    ) = 1
)

, product_hashed as (
    select
          {{ dbt_utils.generate_surrogate_key(['product_code']) }} as product_hkey
        , {{ dbt_utils.generate_surrogate_key([
               'product_code'
             , 'product_name'
             , 'category'
           ]) }} as product_hdiff
           
        , *
    from product_latest
)

select
    *
from product_hashed
