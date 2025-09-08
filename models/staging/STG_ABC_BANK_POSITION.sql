{{ config(materialized='ephemeral') }}

WITH

src_data as (
    SELECT
        UPPER(ACCOUNTID)                           as ACCOUNT_CODE,
        UPPER(SYMBOL)                              as SECURITY_CODE,
        DESCRIPTION                                as SECURITY_NAME,
        UPPER(EXCHANGE)                            as EXCHANGE_CODE,
        {{ normalize_report_date('REPORT_DATE') }} as report_date,
        QUANTITY                                   as QUANTITY,
        COST_BASE                                  as COST_BASE,
        POSITION_VALUE                             as POSITION_VALUE,
        UPPER(CURRENCY)                            as CURRENCY_CODE,

        'SOURCE_DATA.ABC_BANK_POSITION' as RECORD_SOURCE

    FROM {{ source('abc_bank', 'ABC_BANK_POSITION') }}
),

hashed as (
    SELECT
        concat_ws('|', ACCOUNT_CODE, SECURITY_CODE) as POSITION_HKEY,
        concat_ws('|', ACCOUNT_CODE, SECURITY_CODE,
                       SECURITY_NAME, EXCHANGE_CODE,
                       to_varchar(report_date, 'YYYY-MM-DD'), 
                       QUANTITY, COST_BASE, POSITION_VALUE, CURRENCY_CODE ) as POSITION_HDIFF,
        *,
        '{{ run_started_at }}'::timestamp as LOAD_TS_UTC
    FROM src_data
)

SELECT * FROM hashed
