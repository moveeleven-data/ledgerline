{{ config(materialized='ephemeral') }}

WITH

src_data as (
    SELECT
        AlphabeticCode                as CURRENCY_CODE,     -- text
        NumericCode                   as CURRENCY_NUM_CODE, -- text or number
        DecimalDigits                 as DECIMAL_DIGITS,    -- number
        CurrencyName                  as CURRENCY_NAME,     -- text
        Locations                     as LOCATIONS,         -- text
        LOAD_TS                       as LOAD_TS,           -- TIMESTAMP_NTZ

        'SEED.ABC_BANK_CURRENCY_INFO' as RECORD_SOURCE

    FROM {{ source('seeds', 'ABC_Bank_CURRENCY_INFO') }}
),

default_record as (
    SELECT
        '-1'                           as CURRENCY_CODE,
        NULL                           as CURRENCY_NUM_CODE,
        2                              as DECIMAL_DIGITS,
        'Missing'                      as CURRENCY_NAME,
        NULL                           as LOCATIONS,
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
        -- natural key for this dim is alphabetic code
        CURRENCY_CODE as CURRENCY_HKEY,

        -- change fingerprint across descriptive attributes
        CONCAT_WS('|', CURRENCY_CODE, CURRENCY_NAME,
                       DECIMAL_DIGITS, LOCATIONS
        ) as CURRENCY_HDIFF,

        * EXCLUDE LOAD_TS,
        LOAD_TS as LOAD_TS_UTC
    FROM with_default_record
)

SELECT * FROM hashed