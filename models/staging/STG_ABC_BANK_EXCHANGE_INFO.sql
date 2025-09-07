{{ config(materialized='ephemeral') }}

WITH

src_data as (
    SELECT
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

    FROM {{ source('seeds', 'ABC_Bank_EXCHANGE_INFO') }}
),

default_record as (
    SELECT
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
        'Missing'                      as RECORD_SOURCE
),

with_default_record as (
    SELECT * FROM src_data
    UNION ALL
    SELECT * FROM default_record
),

hashed as (
    SELECT
        EXCHANGE_CODE as EXCHANGE_HKEY,
        CONCAT_WS('|', EXCHANGE_CODE, EXCHANGE_NAME, COUNTRY_NAME,
                       CITY_NAME, TIMEZONE_NAME, OPEN_UTC, CLOSE_UTC
        ) as EXCHANGE_HDIFF,
        
        * EXCLUDE LOAD_TS,
        LOAD_TS as LOAD_TS_UTC
    FROM with_default_record
)

SELECT * FROM hashed