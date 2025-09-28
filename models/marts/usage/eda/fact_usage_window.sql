{{ config(
     materialized = 'table'
   , tags = ['mart:usage', 'fact', 'domain:usage_billing', 'eda']
) }}

/**
 * fact_usage_window.sql
 * ---------------------
 * Daily usage and billing fact over a chosen date window at the grain:
 *   customer_key × product_key × plan_key × report_date.
 *
 * Purpose:
 * - Join priced usage with standard dimensions.
 * - Calculate billed, included, and overage values plus overage share.
 * - Persist as a table for EDA visuals across multiple days.
 *
 * Note:
 * - Does not affect the canonical fact_usage, which only holds the latest day.
 */

with

/* Step 1. Bring in priced usage within the window.
   - Uses int__fact_usage_priced_window as the source. */

priced_usage as (
    select
          report_date
        , customer_code_nk
        , product_code_nk
        , plan_code_nk
        , currency_code_nk
        , units_used
        , included_units
        , overage_units
        , unit_price
    from {{ ref('int__fact_usage_priced_window') }}
)

/* Step 2. Add dimension keys and compute billing fields.
   - Join to customer, product, plan, and currency.
   - Derive billed_value, included_value, overage_value, and share of spend from overages. */

, enriched_usage as (
    select
          dim_customer.customer_key                          as customer_key
        , dim_product.product_key                            as product_key
        , dim_plan.plan_key                                  as plan_key
        , dim_currency.currency_key                          as currency_key
        , priced_usage.report_date                           as report_date
        , priced_usage.units_used                            as units_used
        , priced_usage.included_units                        as included_units
        , priced_usage.overage_units                         as overage_units
        , coalesce(priced_usage.unit_price, 0)               as unit_price

        , (priced_usage.units_used     * coalesce(priced_usage.unit_price, 0)) as billed_value
        , (priced_usage.included_units * coalesce(priced_usage.unit_price, 0)) as included_value
        , (priced_usage.overage_units  * coalesce(priced_usage.unit_price, 0)) as overage_value

        , case
              when (priced_usage.units_used      * coalesce(priced_usage.unit_price, 0)) > 0
                then (priced_usage.overage_units * coalesce(priced_usage.unit_price, 0))
                   / (priced_usage.units_used    * coalesce(priced_usage.unit_price, 0))
              else 0
          end as overage_share

    from priced_usage

    join {{ ref('dim_customer') }} dim_customer
      on dim_customer.customer_code = priced_usage.customer_code_nk

    join {{ ref('dim_product') }} dim_product
      on dim_product.product_code = priced_usage.product_code_nk

    join {{ ref('dim_plan') }} dim_plan
      on dim_plan.plan_code = priced_usage.plan_code_nk

    join {{ ref('dim_currency') }} dim_currency
      on dim_currency.currency_code = priced_usage.currency_code_nk
)

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
from enriched_usage
