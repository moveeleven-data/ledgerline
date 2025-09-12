{{ config(materialized='ephemeral') }}

with

src_data as (
    select
        ID              as EXCHANGE_CODE,
        Name            as EXCHANGE_NAME,
        Country         as COUNTRY_NAME,
        City            as CITY_NAME,
        Zone            as TIMEZONE_NAME,
        Delta           as TZ_DELTA_HOURS,
        DST_period      as DST_PERIOD_DESC,
        Open            as OPEN_LOCAL,
        Close           as CLOSE_LOCAL,
        Lunch           as LUNCH_LOCAL,
        Open_UTC        as OPEN_UTC,
        Close_UTC       as CLOSE_UTC,
        Lunch_UTC       as LUNCH_UTC,
        LOAD_TS         as LOAD_TS,

        'SEED.ABC_BANK_EXCHANGE_INFO' as RECORD_SOURCE

    from {{ source('seeds', 'ABC_Bank_EXCHANGE_INFO') }}
),

default_record as (
    select
        '-1'                           as EXCHANGE_CODE,
        'Missing'                      as EXCHANGE_NAME,
        NULL                           as COUNTRY_NAME,
        NULL                           as CITY_NAME,
        NULL                           as TIMEZONE_NAME,
        NULL                           as TZ_DELTA_HOURS,
        NULL                           as DST_PERIOD_DESC,
        NULL                           as OPEN_LOCAL,
        NULL                           as CLOSE_LOCAL,
        NULL                           as LUNCH_LOCAL,
        NULL                           as OPEN_UTC,
        NULL                           as CLOSE_UTC,
        NULL                           as LUNCH_UTC,

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
        {{ dbt_utils.generate_surrogate_key(['exchange_code']) }} as exchange_hkey,

        {{ dbt_utils.generate_surrogate_key([
            'exchange_code',
            'exchange_name',
            'country_name',
            'city_name',
            'timezone_name',
            "coalesce(open_utc,'')",
            "coalesce(close_utc,'')"
        ]) }} as exchange_hdiff,

        * exclude (load_ts),
        load_ts as load_ts_utc

    from with_default_record
)

select * from hashed