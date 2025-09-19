{{ config(tags=['qa']) }}

-- QA Check: Trace event history for sample keys with CLOSE_SYNTHETIC activity
-- Focus: surface timelines where synthetic close events occurred
-- Param: qa_top_n_keys (number of keys to sample, default 3)

{% set qa_top_n_keys = var('qa_top_n_keys', 3) %}

with keys as (
  select
      position_hkey
    , count_if(position_row_type = 'CLOSE_SYNTHETIC') as close_events
  from {{ ref('HIST_ABC_BANK_POSITION') }}
  group by position_hkey
  having close_events > 0
  qualify row_number() over (
     order by close_events desc
  ) <= {{ qa_top_n_keys }}
)

select
    hist.position_hkey
  , hist.report_date
  , hist.position_row_type
  , hist.quantity
  , hist.cost_base
  , hist.position_value
from {{ ref('HIST_ABC_BANK_POSITION') }} hist
join keys on keys.position_hkey = hist.position_hkey
order by hist.position_hkey, hist.report_date, hist.position_row_type