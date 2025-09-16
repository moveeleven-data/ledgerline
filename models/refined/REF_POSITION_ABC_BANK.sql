with

position as (
    {{ current_from_history(
        history_rel = ref('HIST_ABC_BANK_POSITION')
        , key_column = 'POSITION_HKEY'
        , history_filter_expr = "position_row_type = 'OPEN'"
    ) }}
)

, security as (
    select * from {{ ref('REF_SECURITY_ABC_BANK') }}
)

select
    pos.*
    , POSITION_VALUE - COST_BASE as UNREALIZED_PROFIT
    , ROUND(DIV0(UNREALIZED_PROFIT, COST_BASE), 5) * 100 as UNREALIZED_PROFIT_PCT
from position as pos
