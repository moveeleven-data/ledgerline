/**
 * fact_usage.sql
 * --------------
 * Daily usage and billing amounts at the grain
 *   customer_key × product_key × plan_key × report_date.
 *
 * Responsibilities:
 * - Join conformed dimensions, including closed-domain currency.
 * - Compute billed, included, and overage values from priced inputs.
 * - Publish at the declared grain enforced by tests in usage.yml.
 */

with

/* Step 1. Bring in priced usage.
   - All pricing logic isolated upstream in int_fact_usage_priced. */

priced_usage as (
    select
        *
    from {{ ref('int_fact_usage_priced') }}
)

/* Step 2. Join conformed dimensions and compute billing values.
   - Add surrogate keys from dim_customer, dim_product, dim_plan, and dim_currency.
   - Derive billed_value, included_value, and overage_value. */

, enriched_usage as (
    select
          dim_customer.customer_key                                             as customer_key
        , dim_product.product_key                                               as product_key
        , dim_plan.plan_key                                                     as plan_key
        , dim_currency.currency_key                                             as currency_key
        , priced_usage.report_date                                              as report_date
        , priced_usage.units_used                                               as units_used
        , priced_usage.included_units                                           as included_units
        , priced_usage.overage_units                                            as overage_units

        , coalesce(priced_usage.unit_price, 0)                                  as unit_price
        , (priced_usage.units_used     * coalesce(priced_usage.unit_price, 0))  as billed_value
        , (priced_usage.included_units * coalesce(priced_usage.unit_price, 0))  as included_value
        , (priced_usage.overage_units  * coalesce(priced_usage.unit_price, 0))  as overage_value

        , case
              when (priced_usage.units_used * coalesce(priced_usage.unit_price, 0)) > 0
                then (priced_usage.overage_units * coalesce(priced_usage.unit_price, 0))
                   / (priced_usage.units_used * coalesce(priced_usage.unit_price, 0))
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

/* Step 3. Publish the fact.
   - Declared grain enforced by uniqueness test:
     customer_key × product_key × plan_key × report_date.
   - If pricing introduces multi-currency, extend to include currency_key. */

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