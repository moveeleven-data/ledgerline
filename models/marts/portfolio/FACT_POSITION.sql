with

normalized as (
  select
    report_date,
    quantity,
    cost_base,
    position_value,
    unrealized_profit,
    unrealized_profit_pct,
    coalesce(account_code,  '-1') as account_code_nk,
    coalesce(security_code, '-1') as security_code_nk,
    coalesce(exchange_code, '-1') as exchange_code_nk,
    coalesce(currency_code, '-1') as currency_code_nk

  from {{ ref('REF_POSITION_ABC_BANK') }}
),

joined as (
  select
    da.account_key,
    ds.security_key,
    de.exchange_key,
    dc.currency_key,
    n.report_date::date as report_date,
    n.quantity,
    n.cost_base,
    n.position_value,
    n.unrealized_profit,
    n.unrealized_profit_pct

  from normalized n
  join {{ ref('DIM_ACCOUNT')   }} da on n.account_code_nk  = da.account_code
  join {{ ref('DIM_SECURITY')  }} ds on n.security_code_nk = ds.security_code
  join {{ ref('DIM_EXCHANGE')  }} de on n.exchange_code_nk = de.exchange_code
  join {{ ref('DIM_CURRENCY')  }} dc on n.currency_code_nk = dc.currency_code
)
select
  account_key,
  security_key,
  exchange_key,
  currency_key,
  report_date,
  quantity,
  cost_base,
  position_value,
  unrealized_profit,
  unrealized_profit_pct

from joined