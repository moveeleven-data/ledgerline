{{ config(materialized='ephemeral') }}

with

src_positions as (
    select
        upper(accountid)                            as account_code
        , upper(symbol)                             as security_code
        , upper(exchange)                           as exchange_code
        , {{ to_21st_century_date('report_date') }} as report_date
        , quantity                                  as quantity
        , cost_base                                 as cost_base
        , position_value                            as position_value
        , upper(currency)                           as currency_code
        , 'SOURCE_DATA.abc_bank_position'           as record_source
    from {{ source('abc_bank', 'abc_bank_position') }}
)

, security_lookup as (
    select
        upper(security_code) as security_code
        , security_name
    from {{ ref('abc_bank_security_info') }}
)

, enriched_positions as (
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
    from src_positions p
    left join security_lookup l
        on p.security_code = l.security_code
)

, hashed_positions as (
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
        ]) }} as position_hdiff

        , *
        , to_timestamp_ntz('{{ run_started_at }}') as load_ts_utc
    from enriched_positions
)

select * from hashed_positions