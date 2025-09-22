{{ config(tags = ['qa']) }}

-- QA Check: Trace event history for sample keys with CLOSE_SYNTHETIC activity
-- Focus: surface timelines where synthetic close events occurred
-- Param: qa_top_n_keys (number of keys to sample, default 3)

{% set qa_top_n_keys = var('qa_top_n_keys', 3) %}

with keys as (
  select
      usage_hkey
    , count_if(usage_row_type = 'CLOSE_SYNTHETIC') as close_events
  from {{ ref('hist_atlas_meter_usage_daily') }}
  group by
      usage_hkey
  having close_events > 0
  qualify row_number() over (order by close_events desc) <= {{ qa_top_n_keys }}
)

select
    hist.usage_hkey
  , hist.report_date
  , hist.usage_row_type
  , hist.units_used
  , hist.included_units
from {{ ref('hist_atlas_meter_usage_daily') }} hist
join keys
  on keys.usage_hkey = hist.usage_hkey
order by
    hist.usage_hkey
  , hist.report_date
  , hist.usage_row_type;
