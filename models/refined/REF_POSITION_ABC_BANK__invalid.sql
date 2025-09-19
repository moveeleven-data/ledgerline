{{ config(materialized='view') }}

select
    position_hkey
  , account_code
  , security_code
  , report_date
  , quantity
  , cost_base
  , position_value
  , currency_code
from {{ ref('REF_POSITION_ABC_BANK') }}
where
       quantity       is null
    or cost_base      is null
    or position_value is null
    or currency_code  is null
    or cost_base = 0