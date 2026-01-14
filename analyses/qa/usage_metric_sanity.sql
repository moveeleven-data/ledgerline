/**
 * qa__fact_usage_metric_check.sql
 * -------------------------------
 * Recompute billing and margin metrics from base columns in fact_daily_usage
 * and compare them to the stored metric values.
 *
 * Metric definitions:
 * - billed_value   = units_used * unit_price
 *   Total cost of all usage before applying any allowance
 *
 * - included_value = included_units * unit_price
 *   Shows what portion of usage was covered by the plan.
 *
 * - overage_value   = (units_used – included_units) * unit_price
 *   Overage value beyond the allowance.
 *
 * - overage_share     = overage_value / billed_value
 *   How much of the total bill comes from overages.
 *
 * Input:
 * - None (runs across all rows in fact_daily_usage).
 *
 * Output:
 * - Side-by-side comparison of calculated vs stored metrics.
 */

select
    fact_usage.customer_key
  , fact_usage.product_key
  , fact_usage.plan_key
  , fact_usage.report_date
  , fact_usage.units_used
  , fact_usage.included_units
  , fact_usage.unit_price

  , (fact_usage.units_used * fact_usage.unit_price) as calc_billed_value
  , fact_usage.billed_value

  , (fact_usage.included_units * fact_usage.unit_price) as calc_included_value
  , fact_usage.included_value

  , ((fact_usage.units_used - fact_usage.included_units) * fact_usage.unit_price) as calc_overage_value
  , fact_usage.overage_value

  , case
        when (fact_usage.units_used * fact_usage.unit_price) > 0
          then ((fact_usage.units_used - fact_usage.included_units) * fact_usage.unit_price)
               / (fact_usage.units_used * fact_usage.unit_price)
        else 0
    end as calc_overage_share

  , fact_usage.overage_share

from {{ ref('fact_daily_usage') }} as fact_usage
order by
    fact_usage.report_date
  , fact_usage.customer_key
  , fact_usage.product_key
  , fact_usage.plan_key;