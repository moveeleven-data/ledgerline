{{ config(
     materialized = 'table'
   , tags = [
         'mart:usage'
       , 'fact'
       , 'domain:usage_billing'
       , 'eda']
) }}

/**
 * fact_usage_window.sql
 * ---------------------
 * Windowed fact for EDA. Pure projection from the EDA intermediate.
 */

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
from {{ ref('int_fact_usage_priced_window') }}
