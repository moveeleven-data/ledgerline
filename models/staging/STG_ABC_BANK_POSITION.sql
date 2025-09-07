{{ config(materialized='ephemeral') }}

WITH

src_data AS (
    SELECT
        UPPER(ACCOUNTID)      AS ACCOUNT_CODE,
        UPPER(SYMBOL)         AS SECURITY_CODE,
        DESCRIPTION           AS SECURITY_NAME,
        UPPER(EXCHANGE)       AS EXCHANGE_CODE,
        REPORT_DATE           AS REPORT_DATE,
        QUANTITY              AS QUANTITY,
        COST_BASE             AS COST_BASE,
        POSITION_VALUE        AS POSITION_VALUE,
        UPPER(CURRENCY)       AS CURRENCY_CODE,

        'SOURCE_DATA.ABC_BANK_POSITION' AS RECORD_SOURCE

    FROM {{ source('abc_bank', 'ABC_BANK_POSITION') }}
),

hashed as (
    SELECT
        concat_ws('|', ACCOUNT_CODE, SECURITY_CODE) as POSITION_HKEY,
        concat_ws('|', ACCOUNT_CODE, SECURITY_CODE,
                 SECURITY_NAME, EXCHANGE_CODE, REPORT_DATE,
                 QUANTITY, COST_BASE, POSITION_VALUE, CURRENCY_CODE ) as POSITION_HDIFF,
        *,
        '{{ run_started_at }}'::timestamp as LOAD_TS_UTC
    FROM src_data
)

SELECT * FROM hashed
