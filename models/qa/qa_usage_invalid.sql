{{ config(materialized='view') }}

with

src as (
  select
      usage_hkey
    , customer_code
    , product_code
    , plan_code
    , report_date
    , units_used
    , included_units
  from {{ ref('stg_atlas_meter_usage_daily') }}
)

select *
from src
where   report_date    is null
     or units_used     is null
     or included_units is null
     or units_used     < 0
     or included_units < 0