{{ config(materialized='ephemeral') }}

WITH

-- BASE ROWS FROM THE SOURCE FACT FEED
base AS (
    SELECT
        UPPER(ACCOUNTID)  AS ACCOUNT_CODE,
        UPPER(CURRENCY)   AS ACCOUNT_CURRENCY_CODE,
        REPORT_DATE
        
    FROM {{ source('abc_bank', 'ABC_BANK_POSITION') }}
),

-- KEEP THE LATEST CURRENCY PER ACCOUNT BY REPORT DATE
latest AS (
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
            ) AS rn
        FROM base
    )
    QUALIFY rn = 1
),

-- ADD A DEFAULT RECORD SO FACTS CAN ALWAYS JOIN
with_default AS (
    SELECT
        ACCOUNT_CODE,
        ACCOUNT_CURRENCY_CODE,

        'SOURCE_DATA.ABC_BANK_POSITION'     AS RECORD_SOURCE,
        '{{ run_started_at }}'::TIMESTAMP   AS LOAD_TS
    FROM latest

    UNION ALL

    SELECT
        '-1'                           AS ACCOUNT_CODE,
        '-1'                           AS ACCOUNT_CURRENCY_CODE,
        'MISSING'                      AS RECORD_SOURCE,
        TO_TIMESTAMP_NTZ('2020-01-01') AS LOAD_TS
),

-- KEYS AND CHANGE FINGERPRINT FOR SNAPSHOTTING
hashed AS (
    SELECT
        ACCOUNT_CODE AS ACCOUNT_HKEY,
        CONCAT_WS('|', ACCOUNT_CODE, ACCOUNT_CURRENCY_CODE) AS ACCOUNT_HDIFF,

        * EXCLUDE LOAD_TS,
        LOAD_TS AS LOAD_TS_UTC
    FROM with_default
)

SELECT *
FROM hashed