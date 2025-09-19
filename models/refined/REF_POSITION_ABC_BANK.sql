with

position as (
  {{ current_from_history(
      history_rel = ref('HIST_ABC_BANK_POSITION')
    , key_column = 'POSITION_HKEY'
    , history_filter_expr = "
        position_row_type = 'OPEN'
        and quantity is not null
        and cost_base is not null
        and position_value is not null
        and currency_code is not null
        and cost_base <> 0   -- avoid divide-by-zero; keep only calculable rows
      "
  ) }}
)

select
    pos.*
  , (pos.position_value - pos.cost_base)                                      as unrealized_profit
  , round(((pos.position_value - pos.cost_base) / pos.cost_base) * 100, 5)    as unrealized_profit_pct
from position as pos
