{{ config(materialized='table') }}

select
    ACCOUNT_HKEY as ACCOUNT_KEY,
    ACCOUNT_CODE,
    ACCOUNT_CURRENCY_CODE,
    case
        when upper(RECORD_SOURCE) = 'MISSING' then 'Missing'
        else RECORD_SOURCE
    end
    as RECORD_SOURCE,
    LOAD_TS_UTC
from {{ ref('REF_ABC_BANK_ACCOUNT_INFO') }}