{{ config(materialized='ephemeral') }}

with 

src_data as (
    select
        SECURITY_CODE     as SECURITY_CODE,   -- TEXT
        SECURITY_NAME     as SECURITY_NAME,   -- TEXT
        SECTOR            as SECTOR_NAME,     -- TEXT
        INDUSTRY          as INDUSTRY_NAME,   -- TEXT
        COUNTRY           as COUNTRY_CODE,    -- TEXT
        EXCHANGE          as EXCHANGE_CODE,   -- TEXT
        LOAD_TS           as LOAD_TS,         -- TIMESTAMP_NTZ

        'SEED.ABC_Bank_SECURITY_INFO' as RECORD_SOURCE

    from {{ source('seeds', 'ABC_Bank_SECURITY_INFO') }}
 ),

default_record as (
    select
          '-1'         as SECURITY_CODE
        , 'Missing'    as SECURITY_NAME
        , 'Missing'    as SECTOR_NAME
        , 'Missing'    as INDUSTRY_NAME
        , '-1'         as COUNTRY_CODE
        , '-1'         as EXCHANGE_CODE
        , '2020-01-01' as LOAD_TS_UTC
        , 'Missing'    as RECORD_SOURCE
),

with_default_record as(
    select * from src_data
    union all
    select * from default_record
),

hashed as (
    select
        {{ dbt_utils.surrogate_key(['security_code']) }} as security_hkey,

        {{ dbt_utils.surrogate_key([
            'security_code',
            'security_name',
            'sector_name',
            'industry_name',
            'country_code',
            'exchange_code'
        ]) }} as security_hdiff,

        * exclude load_ts,
        load_ts as load_ts_utc

    from with_default_record
)

select * from hashed