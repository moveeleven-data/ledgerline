{{ config(materialized='ephemeral') }}

WITH

src_data AS (
    SELECT
        AlphabeticCode      AS CURRENCY_CODE,     -- text
        NumericCode         AS CURRENCY_NUM_CODE, -- text or number
        DecimalDigits       AS DECIMAL_DIGITS,    -- number
        CurrencyName        AS CURRENCY_NAME,     -- text
        Locations           AS LOCATIONS,         -- text
        LOAD_TS             AS LOAD_TS_UTC,       -- TIMESTAMP_NTZ
        'SEED.ABC_BANK_CURRENCY' AS RECORD_SOURCE

    FROM {{ source('seeds', 'ABC_Bank_CURRENCY_INFO') }}
),

default_record AS (
    SELECT
        '-1'                           AS CURRENCY_CODE,
        NULL                           AS CURRENCY_NUM_CODE,
        2                              AS DECIMAL_DIGITS,
        'Missing'                      AS CURRENCY_NAME,
        NULL                           AS LOCATIONS,
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
        -- natural key for this dim is alphabetic code
        CURRENCY_CODE AS CURRENCY_HKEY,

        -- change fingerprint across descriptive attributes
        CONCAT_WS('|', CURRENCY_CODE, CURRENCY_NAME,
                       DECIMAL_DIGITS, LOCATIONS
        ) AS CURRENCY_HDIFF,
        *
    FROM with_default_record
)

SELECT * FROM hashed