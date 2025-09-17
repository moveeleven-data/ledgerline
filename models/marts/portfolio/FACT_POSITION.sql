with

position_normalized as (
    select
        report_date
        , quantity
        , cost_base
        , position_value
        , unrealized_profit
        , unrealized_profit_pct
        , coalesce(account_code,  '-1') as account_code_nk
        , coalesce(security_code, '-1') as security_code_nk
        , coalesce(exchange_code, '-1') as exchange_code_nk
        , coalesce(currency_code, '-1') as currency_code_nk
    from {{ ref('REF_POSITION_ABC_BANK') }}
)

, position_enriched as (
  select
      dim_account.account_key
      , dim_security.security_key   as security_key
      , dim_exchange.exchange_key
      , dim_currency.currency_key
      , pos_norm.report_date::date  as report_date
      , pos_norm.quantity
      , pos_norm.cost_base
      , pos_norm.position_value
      , pos_norm.unrealized_profit
      , pos_norm.unrealized_profit_pct
  from position_normalized pos_norm
  -- left joins so defaulted '-1' facts do not drop when dims hide default rows
  left join {{ ref('DIM_ACCOUNT')  }}  dim_account
    on pos_norm.account_code_nk  = dim_account.account_code
  left join {{ ref('DIM_SECURITY') }}  dim_security
    on pos_norm.security_code_nk = dim_security.security_code
  left join {{ ref('DIM_EXCHANGE') }}  dim_exchange
    on pos_norm.exchange_code_nk = dim_exchange.exchange_code
  left join {{ ref('DIM_CURRENCY') }}  dim_currency
    on pos_norm.currency_code_nk = dim_currency.currency_code
)

select
    account_key
    , security_key
    , exchange_key
    , currency_key
    , report_date
    , quantity
    , cost_base
    , position_value
    , unrealized_profit
    , unrealized_profit_pct
from position_enriched