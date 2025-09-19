with

position_normalized as (
    select
          report_date::date                                   as report_date
        , cast(quantity as number(38,0))                      as quantity
        , cast(cost_base as number(38,2))                     as cost_base
        , cast(position_value as number(38,2))                as position_value
        , cast(position_value - cost_base as number(38,2))    as unrealized_profit
        , cast(
             round(
                 (position_value - cost_base)
                 / nullif(cost_base, 0)
                 * 100
                 , 5
             ) as number(38,5)
        ) as unrealized_profit_pct
        
        -- Normalize natural keys; late/missing values go to the Unknown member
        , coalesce(account_code,  '-1') as account_code_nk
        , coalesce(security_code, '-1') as security_code_nk
        , coalesce(exchange_code, '-1') as exchange_code_nk
        , coalesce(currency_code, '-1') as currency_code_nk
    from {{ ref('REF_POSITION_ABC_BANK') }}
)

, position_enriched as (
  select
        dim_account.account_key         as account_key
      , dim_security.security_key       as security_key
      , dim_exchange.exchange_key       as exchange_key
      , dim_currency.currency_key       as currency_key
      , pos_norm.report_date            as report_date
      , pos_norm.quantity               as quantity
      , pos_norm.cost_base              as cost_base
      , pos_norm.position_value         as position_value
      , pos_norm.unrealized_profit      as unrealized_profit
      , pos_norm.unrealized_profit_pct  as unrealized_profit_pct

  from position_normalized pos_norm

  join {{ ref('DIM_ACCOUNT')  }}  dim_account
    on dim_account.account_code = pos_norm.account_code_nk

  join {{ ref('DIM_SECURITY') }}  dim_security
    on dim_security.security_code = pos_norm.security_code_nk

  join {{ ref('DIM_EXCHANGE') }}  dim_exchange
    on dim_exchange.exchange_code = pos_norm.exchange_code_nk

  join {{ ref('DIM_CURRENCY') }}  dim_currency
    on dim_currency.currency_code = pos_norm.currency_code_nk
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