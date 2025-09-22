{{ config(severity='error') }}

{% set as_of_str  = get_latest_usage_report_date() %}
{% set as_of_expr = "to_date('" ~ as_of_str ~ "')" %}

with prior_latest_open as (
  select distinct
        customer_code
      , product_code
      , plan_code
  from {{ ref('hist_atlas_meter_usage_daily') }}
  where usage_row_type = 'OPEN'
    and report_date    < {{ as_of_expr }}
)

, asof_rows as (
  select distinct
        customer_code
      , product_code
      , plan_code
  from {{ ref('hist_atlas_meter_usage_daily') }}
  where report_date = {{ as_of_expr }}
)

, missing as (
  select
        prior_latest_open.customer_code
      , prior_latest_open.product_code
      , prior_latest_open.plan_code
  from prior_latest_open
  left join asof_rows
    using (customer_code, product_code, plan_code)
  where asof_rows.customer_code is null
)

select *
from missing
