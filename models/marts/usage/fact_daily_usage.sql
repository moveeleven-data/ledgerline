{{ config(materialized='table') }}

/**
 * fact_daily_usage.sql
 * ---------------
 * Compute daily usage metrics with pricing at the customer × product × plan × date grain.
 */

with

usage as (
    select
          report_date
        , units_used
        , included_units
        , overage_units
        , customer_key
        , product_key
        , plan_key
        , usage_key
    from {{ ref('ref_usage_atlas') }}
)

, usage_with_price as (
    select
          usage.report_date
        , usage.customer_key
        , usage.product_key
        , usage.plan_key
        , usage.units_used
        , usage.included_units
        , usage.overage_units
        , coalesce(price.unit_price, 0) as unit_price

    from usage
    left join {{ ref('ref_price_book_daily') }} as price
        on price.product_key  = usage.product_key
        and price.plan_key    = usage.plan_key
        and price.price_date <= usage.report_date

    qualify row_number() over (
        partition by
            usage.report_date
          , usage.customer_key
          , usage.product_key
          , usage.plan_key
        order by
            price.price_date desc nulls last
    ) = 1
)

, usage_with_value_metrics as (
    select
          usage_with_price.*
        , (units_used     * unit_price) as billed_value
        , (included_units * unit_price) as included_value
        , (overage_units  * unit_price) as overage_value
        , case when (units_used * unit_price) > 0
            then (overage_units * unit_price)
                 / (units_used * unit_price)
            else 0
          end as overage_share
    from usage_with_price
)

select
      report_date
    , usage_key
    , customer_key
    , product_key
    , plan_key
    , units_used
    , included_units
    , overage_units
    , unit_price
    , billed_value
    , included_value
    , overage_value
    , overage_share
from usage_with_value_metrics
