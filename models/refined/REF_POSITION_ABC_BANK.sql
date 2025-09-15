with

position as (
    {{ current_from_history(
        history_rel = ref('HIST_ABC_BANK_POSITION_WITH_CLOSING'), 
        key_column = 'POSITION_HKEY',
    ) }}
)
, security as (
    select * from {{ ref('REF_SECURITY_INFO_ABC_BANK') }}
)

select
    p.* exclude (SECURITY_CODE)
    , coalesce(s.SECURITY_CODE, '-1') as SECURITY_CODE
    , POSITION_VALUE - COST_BASE as UNREALIZED_PROFIT
    , ROUND(DIV0(UNREALIZED_PROFIT, COST_BASE), 5)*100 as UNREALIZED_PROFIT_PCT
from position as p
left outer join security as s
    on(s.SECURITY_CODE = p.SECURITY_CODE)
