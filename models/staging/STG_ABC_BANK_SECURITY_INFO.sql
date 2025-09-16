{{ config(materialized='ephemeral') }}

with src_data as (
    select
        security_code   as security_code,
        security_name   as security_name,
        sector          as sector_name,
        industry        as industry_name,
        country         as country_code,
        exchange        as exchange_code,
        load_ts         as load_ts,

        'SEED.abc_bank_security_info' as record_source
    from {{ ref('abc_bank_security_info') }}
),

default_record as (
    select
        '-1'                            as security_code,
        'Missing'                       as security_name,
        'Missing'                       as sector_name,
        'Missing'                       as industry_name,
        '-1'                            as country_code,
        '-1'                            as exchange_code,
        to_timestamp_ntz('2020-01-01')  as load_ts,
        'System.DefaultKey'             as record_source
),

with_default_record as(
    select * from src_data
    union all
    select * from default_record
),

hashed as (
    select
        {{ dbt_utils.generate_surrogate_key(['security_code']) }} as security_hkey,
        {{ dbt_utils.generate_surrogate_key([
            'security_code',
            'security_name',
            'sector_name',
            'industry_name',
            'country_code',
            'exchange_code'
        ]) }} as security_hdiff,

        * exclude (load_ts),
        
        load_ts as load_ts_utc
    from with_default_record
)

select * from hashed