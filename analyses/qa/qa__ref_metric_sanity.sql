{{ config(tags=['qa']) }}

-- QA Check: Recalculate unrealized metrics and validate against REF_POSITION_ABC_BANK
-- Focus: compare recomputed values (calc_unrealized, calc_pct) to stored columns
-- Expectation: values should align within tolerance, discrepancies indicate issues

select
    position_hkey
  , cost_base
  , position_value
  , (position_value - cost_base) as calc_unrealized
  , unrealized_profit
  , round((position_value - cost_base) / nullif(cost_base,0) * 100, 5) as calc_pct
  , unrealized_profit_pct
from {{ ref('REF_POSITION_ABC_BANK') }}
order by position_hkey