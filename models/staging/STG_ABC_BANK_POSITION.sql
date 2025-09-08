{{ config(materialized='ephemeral') }}

with

src_data as (
    SELECT
        UPPER(ACCOUNTID)                           as ACCOUNT_CODE,
        UPPER(SYMBOL)                              as SECURITY_CODE,
        DESCRIPTION                                as SECURITY_NAME,
        UPPER(EXCHANGE)                            as EXCHANGE_CODE,
        {{ to_21st_century_date('REPORT_DATE') }}  as report_date,
        QUANTITY                                   as QUANTITY,
        COST_BASE                                  as COST_BASE,
        POSITION_VALUE                             as POSITION_VALUE,
        UPPER(CURRENCY)                            as CURRENCY_CODE,

        'SOURCE_DATA.ABC_BANK_POSITION' as RECORD_SOURCE

    from {{ source('abc_bank', 'ABC_BANK_POSITION') }}
),

hashed as (
    select
        {{ dbt_utils.surrogate_key(['account_code','security_code']) }} as position_hkey,

        {{ dbt_utils.surrogate_key([
            'account_code',
            'security_code',
            'security_name',
            'exchange_code',
            "to_varchar(report_date, 'YYYY-MM-DD')",
            'quantity',
            'cost_base',
            'position_value',
            'currency_code'
        ]) }} as position_hdiff,

        *,
        '{{ run_started_at }}' as LOAD_TS_UTC
        
    from src_data
)

select * from hashed
