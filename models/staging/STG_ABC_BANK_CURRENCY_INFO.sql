{{ config(materialized='ephemeral') }}

with

src_currencies as (
    select
        AlphabeticCode                 as currency_code
      , NumericCode                    as currency_num_code
      , DecimalDigits                  as decimal_digits
      , CurrencyName                   as currency_name
      , Locations                      as locations
      , load_ts                        as load_ts
      , 'SEED.abc_bank_currency_info'  as record_source
    from {{ ref('abc_bank_currency_info') }}
)

, default_currency_record as (
    select
        '-1'                            as currency_code
      , null                            as currency_num_code
      , 2                               as decimal_digits
      , 'Missing'                       as currency_name
      , null                            as locations
      , to_timestamp_ntz('2020-01-01')  as load_ts
      , 'System.DefaultKey'             as record_source
)

, currencies_with_default as (
    select * from src_currencies
    union all
    select * from default_currency_record
)

, hashed_currencies as (
    select
        {{ dbt_utils.generate_surrogate_key(['currency_code']) }} as currency_hkey
      , {{ dbt_utils.generate_surrogate_key([
              'currency_code'
            , 'currency_name'
            , 'decimal_digits'
            , 'locations']
        ) }} as currency_hdiff

      , * exclude (load_ts)
      , load_ts as load_ts_utc
    from currencies_with_default
)

select * from hashed_currencies