-- fact_usage.sql
-- Grain: customer_key × product_key × plan_key × report_date.

select
      customer_key
    , product_key
    , plan_key
    , currency_key
    , report_date
    , units_used
    , included_units
    , overage_units
    , unit_price
    , billed_value
    , included_value
    , overage_value
    , overage_share
from {{ ref('int_fact_usage_priced') }}