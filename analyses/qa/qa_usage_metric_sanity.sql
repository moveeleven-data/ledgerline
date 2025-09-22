{{ config(tags = ['qa']) }}

-- QA Check: Recalculate billing and margin metrics and validate against FACT_USAGE
-- Focus: compare recomputed values to stored columns
-- Expectation: values should align within tolerance

select
    f.customer_key
  , f.product_key
  , f.plan_key
  , f.report_date
  , f.units_used
  , f.included_units
  , f.unit_price

  , (f.units_used * f.unit_price)                         as calc_billed_value
  , f.billed_value
  , (f.included_units * f.unit_price)                     as calc_included_value
  , f.included_value
  , ((f.units_used - f.included_units) * f.unit_price)    as calc_margin_value
  , f.margin_value

  , case
        when (f.units_used * f.unit_price) > 0
          then ((f.units_used - f.included_units) * f.unit_price)
               / (f.units_used * f.unit_price)
        else 0
    end                                                   as calc_margin_pct
  , f.margin_pct
from {{ ref('fact_usage') }} f
order by
    f.report_date
  , f.customer_key
  , f.product_key
  , f.plan_key;
