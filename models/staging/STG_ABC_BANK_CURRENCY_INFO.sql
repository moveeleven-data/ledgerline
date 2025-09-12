{{ config(materialized='ephemeral') }}

with

src_data as (
    select
        AlphabeticCode                as CURRENCY_CODE,     -- text
        NumericCode                   as CURRENCY_NUM_CODE, -- text or number
        DecimalDigits                 as DECIMAL_DIGITS,    -- number
        CurrencyName                  as CURRENCY_NAME,     -- text
        Locations                     as LOCATIONS,         -- text
        LOAD_TS                       as LOAD_TS,           -- TIMESTAMP_NTZ

        'SEED.ABC_BANK_CURRENCY_INFO' as RECORD_SOURCE

    from {{ source('seeds', 'ABC_Bank_CURRENCY_INFO') }}
),

default_record as (
    select
        '-1'                           as CURRENCY_CODE,
        NULL                           as CURRENCY_NUM_CODE,
        2                              as DECIMAL_DIGITS,
        'Missing'                      as CURRENCY_NAME,
        NULL                           as LOCATIONS,
        TO_TIMESTAMP_NTZ('2020-01-01') as LOAD_TS_UTC,
        'System.DefaultKey'            as RECORD_SOURCE
),

with_default_record as (
    select * from src_data
    union all
    select * from default_record
),

hashed as (
    select
        {{ dbt_utils.generate_surrogate_key(['currency_code']) }} as currency_hkey,

        {{ dbt_utils.generate_surrogate_key([
            'currency_code',
            'currency_name',
            'decimal_digits',
            'locations'
        ]) }} as currency_hdiff,

        * exclude (load_ts),
        load_ts as load_ts_utc
        
    from with_default_record
)

select * from hashed