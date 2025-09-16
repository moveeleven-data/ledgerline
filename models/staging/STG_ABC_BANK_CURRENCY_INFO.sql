{{ config(materialized='ephemeral') }}

with

with src_data as (
    select
        AlphabeticCode  as currency_code,
        NumericCode     as currency_num_code,
        DecimalDigits   as decimal_digits,
        CurrencyName    as currency_name,
        Locations       as locations,
        load_ts         as load_ts,

        'SEED.abc_bank_currency_info' as record_source
    from {{ ref('abc_bank_currency_info') }}
),

default_record as (
    select
        '-1'                            as currency_code,
        null                            as currency_num_code,
        2                               as decimal_digits,
        'Missing'                       as currency_name,
        null                            as locations,
        to_timestamp_ntz('2020-01-01')  as load_ts,
        'System.DefaultKey'             as record_source
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
            'locations']
        ) }} as currency_hdiff,

        * exclude (load_ts),
        
        load_ts as load_ts_utc
    from with_default_record
)

select * from hashed