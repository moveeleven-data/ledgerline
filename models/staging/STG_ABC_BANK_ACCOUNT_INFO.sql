{{ config(materialized='ephemeral') }}

WITH

-- base rows from the source fact feed
base as (
    SELECT
        UPPER(ACCOUNTID)  as ACCOUNT_CODE,
        UPPER(CURRENCY)   as ACCOUNT_CURRENCY_CODE,
        REPORT_DATE
        
    FROM {{ source('abc_bank', 'ABC_BANK_POSITION') }}
),

-- keep the latest currency per account by report date
latest as (
    SELECT
        ACCOUNT_CODE,
        ACCOUNT_CURRENCY_CODE

    FROM (
        SELECT
            ACCOUNT_CODE,
            ACCOUNT_CURRENCY_CODE,
            REPORT_DATE,
            ROW_NUMBER() OVER (
                PARTITION BY ACCOUNT_CODE
                ORDER BY REPORT_DATE DESC, ACCOUNT_CURRENCY_CODE
            ) as rn
        FROM base
    )
    QUALIFY rn = 1
),

-- add a default record so facts can always join
with_default as (
    SELECT
        ACCOUNT_CODE,
        ACCOUNT_CURRENCY_CODE,

        'SOURCE_DATA.ABC_BANK_POSITION'     as RECORD_SOURCE,
        '{{ run_started_at }}'::TIMESTAMP   as LOAD_TS
    FROM latest

    UNION ALL

    SELECT
        '-1'                           as ACCOUNT_CODE,
        '-1'                           as ACCOUNT_CURRENCY_CODE,
        'MISSING'                      as RECORD_SOURCE,
        TO_TIMESTAMP_NTZ('2020-01-01') as LOAD_TS
),

-- KEYS AND CHANGE FINGERPRINT FOR SNAPSHOTTING
hashed as (
    SELECT
        ACCOUNT_CODE as ACCOUNT_HKEY,
        CONCAT_WS('|', ACCOUNT_CODE, ACCOUNT_CURRENCY_CODE) as ACCOUNT_HDIFF,

        * EXCLUDE LOAD_TS,
        LOAD_TS as LOAD_TS_UTC
    FROM with_default
)

SELECT *
FROM hashed
