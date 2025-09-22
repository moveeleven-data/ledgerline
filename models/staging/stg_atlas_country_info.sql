{{ config(materialized='ephemeral') }}

with

src as (
    select
          upper(country_code)               as country_code2
        , upper(country_code)               as country_code3
        , country_name
        , to_timestamp_ntz(load_ts)         as load_ts
        , 'SEED.atlas_ref_country_info'     as record_source
    from {{ ref('countries') }}
)

, default_row as (
    select
          '-1'                           as country_code2
        , '-1'                           as country_code3
        , 'Missing'                      as country_name
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
          {{ dbt_utils.generate_surrogate_key(['country_code2']) }} as country_hkey
        , {{ dbt_utils.generate_surrogate_key([
               'country_code2'
              ,'country_code3'
              ,'country_name'
          ]) }} as country_hdiff

        , * exclude (load_ts)
        , load_ts as load_ts_utc
    from unioned
)

select * from hashed
