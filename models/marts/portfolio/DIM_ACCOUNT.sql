{{ config(materialized='table') }}

select
    ACCOUNT_HKEY as ACCOUNT_KEY,
    ACCOUNT_CODE,
    ACCOUNT_CURRENCY_CODE,
    CASE
        WHEN UPPER(RECORD_SOURCE) = 'MISSING' THEN 'Missing' ELSE RECORD_SOURCE
        END AS RECORD_SOURCE,
    LOAD_TS_UTC
from {{ ref('REF_ABC_BANK_ACCOUNT_INFO') }}
