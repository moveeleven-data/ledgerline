{{ config(tags=['qa']) }}

-- QA Check: Ensure no invalid rows appear in REF_POSITION_ABC_BANK
-- Expectation: result set should be empty (any rows flagged as leaked_invalid)

with invalid as (
  select position_hkey
  from {{ ref('REF_POSITION_ABC_BANK__invalid') }}
)

, ref as (
  select *
  from {{ ref('REF_POSITION_ABC_BANK') }}
)

select
    ref.*
  , 'leaked_invalid' as leak_flag
from ref
join invalid using (position_hkey)
order by ref.report_date desc, ref.position_hkey