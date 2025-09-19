with

base as (
  -- keep only OPEN positions before choosing latest
  select *
  from {{ ref('HIST_ABC_BANK_POSITION') }}
  where position_row_type = 'OPEN'
    and quantity        is not null
    and cost_base       is not null
    and position_value  is not null
    and currency_code   is not null
    and cost_base <> 0
)

, position as (
  select *
  from (
    select b.*
           , row_number() over (
             partition by b.position_hkey
             order by
                 b.report_date desc
               , b.load_ts_utc desc
             ) as rn
    from base b
  )
  where rn = 1
)

select
    pos.*
  , (pos.position_value - pos.cost_base) as unrealized_profit
  , round(
      (pos.position_value - pos.cost_base) / pos.cost_base * 100
     , 5
    ) as unrealized_profit_pct
from position pos