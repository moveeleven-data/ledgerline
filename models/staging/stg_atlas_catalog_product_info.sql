{{ config(materialized='ephemeral') }}

with

src as (
    select
          upper(product_code)               as product_code
        , product_name
        , category
        , to_timestamp_ntz(load_ts)         as load_ts
        , 'SEED.atlas_catalog_product_info' as record_source
    from {{ ref('atlas_catalog_product_info') }}
)

, default_row as (
    select
          '-1'                           as product_code
        , 'Missing'                      as product_name
        , 'Missing'                      as category
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
          {{ dbt_utils.generate_surrogate_key(['product_code']) }} as product_hkey
        , {{ dbt_utils.generate_surrogate_key([
               'product_code'
              ,'product_name'
              ,'category'
          ]) }} as product_hdiff

        , * exclude (load_ts)
        , to_timestamp_ntz('{{ run_started_at }}') as load_ts_utc
    from unioned
)

select
    *
from hashed
