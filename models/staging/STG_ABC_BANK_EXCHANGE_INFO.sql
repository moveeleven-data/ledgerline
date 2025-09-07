{{ config(materialized='ephemeral') }}

WITH

src_data AS (
    SELECT
        ID              AS EXCHANGE_CODE,
        Name            AS EXCHANGE_NAME,
        Country         AS COUNTRY_NAME,
        City            AS CITY_NAME,
        Zone            AS TIMEZONE_NAME,
        Delta           AS TZ_DELTA_HOURS,
        DST_period      AS DST_PERIOD_DESC,
        Open            AS OPEN_LOCAL,
        Close           AS CLOSE_LOCAL,
        Lunch           AS LUNCH_LOCAL,
        Open_UTC        AS OPEN_UTC,
        Close_UTC       AS CLOSE_UTC,
        Lunch_UTC       AS LUNCH_UTC,
        LOAD_TS         AS LOAD_TS,

        'SEED.ABC_BANK_EXCHANGE_INFO' AS RECORD_SOURCE

    FROM {{ source('seeds', 'ABC_Bank_EXCHANGE_INFO') }}
),

default_record AS (
    SELECT
        '-1'                           AS EXCHANGE_CODE,
        'Missing'                      AS EXCHANGE_NAME,
        NULL                           AS COUNTRY_NAME,
        NULL                           AS CITY_NAME,
        NULL                           AS TIMEZONE_NAME,
        NULL                           AS TZ_DELTA_HOURS,
        NULL                           AS DST_PERIOD_DESC,
        NULL                           AS OPEN_LOCAL,
        NULL                           AS CLOSE_LOCAL,
        NULL                           AS LUNCH_LOCAL,
        NULL                           AS OPEN_UTC,
        NULL                           AS CLOSE_UTC,
        NULL                           AS LUNCH_UTC,

        TO_TIMESTAMP_NTZ('2020-01-01') AS LOAD_TS_UTC,
        'Missing'                      AS RECORD_SOURCE
),

with_default_record AS (
    SELECT * FROM src_data
    UNION ALL
    SELECT * FROM default_record
),

hashed AS (
    SELECT
        EXCHANGE_CODE AS EXCHANGE_HKEY,
        CONCAT_WS('|', EXCHANGE_CODE, EXCHANGE_NAME, COUNTRY_NAME,
                       CITY_NAME, TIMEZONE_NAME, OPEN_UTC, CLOSE_UTC
        ) AS EXCHANGE_HDIFF,
        
        * EXCLUDE LOAD_TS,
        LOAD_TS AS LOAD_TS_UTC
    FROM with_default_record
)

SELECT * FROM hashed