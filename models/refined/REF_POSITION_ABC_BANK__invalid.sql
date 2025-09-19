{{ config(materialized='view') }}

with latest_open as (
  select *
  from {{ ref('HIST_ABC_BANK_POSITION') }}
  where position_row_type = 'OPEN'
  qualify row_number() over (
    partition by position_hkey
    order by
        report_date desc
      , load_ts_utc desc
  ) = 1
)
select
    position_hkey
  , account_code
  , security_code
  , report_date
  , quantity
  , cost_base
  , position_value
  , currency_code
from latest_open
where
       quantity       is null
    or cost_base      is null
    or position_value is null
    or currency_code  is null
    or cost_base = 0
