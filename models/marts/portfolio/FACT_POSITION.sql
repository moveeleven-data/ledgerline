{{ config(materialized='table') }}

with

normalized as (
    select
        *,
        coalesce(account_code,  '-1') as account_code_nk,
        coalesce(security_code, '-1') as security_code_nk,
        coalesce(exchange_code, '-1') as exchange_code_nk,
        coalesce(currency_code, '-1') as currency_code_nk

    from {{ ref('REF_POSITION_ABC_BANK') }}
),

joined as (
    select
        n.report_date,
        n.quantity,
        n.cost_base,
        n.position_value,
        n.unrealized_profit,
        n.unrealized_profit_pct,
        n.record_source,
        n.load_ts_utc,

        da.account_key,
        ds.security_hkey  as security_key,
        de.exchange_hkey  as exchange_key,
        dc.currency_hkey  as currency_key

    from normalized n
    join {{ ref('DIM_ACCOUNT')  }} da on n.account_code_nk  = da.account_code
    join {{ ref('DIM_SECURITY') }} ds on n.security_code_nk = ds.security_code
    join {{ ref('DIM_EXCHANGE') }} de on n.exchange_code_nk = de.exchange_code
    join {{ ref('DIM_CURRENCY') }} dc on n.currency_code_nk = dc.currency_code
)

select *
from joined
