/**
 * qa__fact_usage_metric_check.sql
 * -------------------------------
 * Recompute billing and margin metrics from base columns in FACT_USAGE
 * and compare them to the stored metric values.
 *
 * Metric definitions:
 * - billed_value   = units_used * unit_price
 *   Total cost of all usage before applying any allowance
 *
 * - included_value = included_units * unit_price
 *   Shows what portion of usage was covered by the plan.
 *
 * - margin_value   = (units_used – included_units) * unit_price
 *   Overage value beyond the allowance.
 *
 * - margin_pct     = margin_value / billed_value
 *   How much of the total bill comes from overages.
 *
 * Input:
 * - None (runs across all rows in FACT_USAGE).
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

  , ((fact_usage.units_used - fact_usage.included_units) * fact_usage.unit_price) as calc_margin_value
  , fact_usage.margin_value

  , case
        when (fact_usage.units_used * fact_usage.unit_price) > 0
          then ((fact_usage.units_used - fact_usage.included_units) * fact_usage.unit_price)
               / (fact_usage.units_used * fact_usage.unit_price)
        else 0
    end as calc_margin_pct

  , fact_usage.margin_pct

from {{ ref('fact_usage') }} as fact_usage
order by
    fact_usage.report_date
  , fact_usage.customer_key
  , fact_usage.product_key
  , fact_usage.plan_key;