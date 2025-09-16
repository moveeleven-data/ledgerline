{{ config(materialized='ephemeral') }}

with

src_data as (
    SELECT
        UPPER(ACCOUNTID)                           as ACCOUNT_CODE,
        UPPER(SYMBOL)                              as SECURITY_CODE,
        UPPER(EXCHANGE)                            as EXCHANGE_CODE,
        {{ to_21st_century_date('REPORT_DATE') }}  as report_date,
        QUANTITY                                   as QUANTITY,
        COST_BASE                                  as COST_BASE,
        POSITION_VALUE                             as POSITION_VALUE,
        UPPER(CURRENCY)                            as CURRENCY_CODE,

        'SOURCE_DATA.ABC_BANK_POSITION' as RECORD_SOURCE

    from {{ source('abc_bank', 'abc_bank_position') }}
),

sec as (
    select
        upper(security_code) as security_code,
        security_name
    from {{ ref('abc_bank_security_info') }}
),

with_name as (
    select
        s.account_code,
        s.security_code,
        coalesce(sec.security_name, 'Missing') as security_name,
        s.exchange_code,
        s.report_date,
        s.quantity,
        s.cost_base,
        s.position_value,
        s.currency_code,
        s.record_source
    from src s
    left join sec
      on s.security_code = sec.security_code
),

hashed as (
    select
        {{ dbt_utils.generate_surrogate_key(['account_code','security_code']) }} as position_hkey,
        {{ dbt_utils.generate_surrogate_key([
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
        
        to_timestamp_ntz('{{ run_started_at }}') as load_ts_utc
    from with_name
)

select * from hashed