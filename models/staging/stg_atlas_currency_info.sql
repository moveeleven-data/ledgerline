{{ config(materialized='ephemeral') }}

with

src as (
    select
          upper(currency_code)              as currency_code
        , currency_name
        , decimal_digits
        , to_timestamp_ntz(load_ts)         as load_ts
        , 'SEED.atlas_ref_currency_info'    as record_source
    from {{ ref('atlas_currency_info') }}
)

, default_row as (
    select
          '-1'                           as currency_code
        , 'Missing'                      as currency_name
        , 2                              as decimal_digits
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
          {{ dbt_utils.generate_surrogate_key(['currency_code']) }} as currency_hkey
        , {{ dbt_utils.generate_surrogate_key([
               'currency_code'
              ,'currency_name'
              ,'decimal_digits'
          ]) }} as currency_hdiff

        , * exclude (load_ts)
        , to_timestamp_tz('{{ run_started_at }}') as load_ts_utc
    from unioned
)

select * from hashed
