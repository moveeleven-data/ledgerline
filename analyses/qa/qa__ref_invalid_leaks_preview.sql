{{ config(tags = ['qa']) }}

-- QA Check: Ensure no invalid rows appear in REF_USAGE_ATLAS
-- Expectation: result set should be empty (any rows flagged as leaked_invalid)

with invalid as (
  select usage_hkey
  from {{ ref('qa_usage_invalid') }}
)

, ref as (
  select  *
  from {{ ref('ref_usage_atlas') }}
)

select
    ref.*
  , 'leaked_invalid' as leak_flag
from ref
join invalid using (usage_hkey)
order by
    ref.report_date desc
  , ref.usage_hkey;
