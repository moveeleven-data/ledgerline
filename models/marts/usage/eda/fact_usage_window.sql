{{ config(
     materialized = 'table'
   , tags = [
         'mart:usage'
       , 'fact'
       , 'domain:usage_billing'
       , 'eda'
     ]
) }}

/**
 * fact_usage_window.sql
 * ---------------------
 * Windowed fact for EDA.
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
    , coalesce(unit_price, 0) as unit_price
    , (units_used     * coalesce(unit_price, 0)) as billed_value
    , (included_units * coalesce(unit_price, 0)) as included_value
    , (overage_units  * coalesce(unit_price, 0)) as overage_value

    , case when (units_used * coalesce(unit_price, 0)) > 0
           then (overage_units * coalesce(unit_price, 0)) 
           / (units_used * coalesce(unit_price, 0))
        else 0
    end as overage_share
from {{ ref('int_fact_usage_priced_window') }}
