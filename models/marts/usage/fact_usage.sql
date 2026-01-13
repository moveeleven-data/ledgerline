{{ config(materialized='table') }}

/**
 * int_fact_usage_priced.sql
 * -------------------------
 * Price latest usage at NK grain.
 * - Compute billed/included/overage values so the fact model can be a pure SELECT.
 */

with

normalized_usage as (
    select
          report_date::date                        as report_date
        , cast(units_used as number(38, 0))        as units_used
        , cast(included_units as number(38, 0))    as included_units
        , greatest(units_used - included_units, 0) as overage_units
        , customer_key                             as customer_key
        , product_key                              as product_key
        , plan_key                                 as plan_key
        , product_code                             as product_code_nk
        , plan_code                                as plan_code_nk
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
        , price.currency_key as currency_key

    from normalized_usage as usage
    left join {{ ref('ref_price_book_daily') }} as price
           on price.product_code = usage.product_code_nk
          and price.plan_code    = usage.plan_code_nk
          and price.price_date  <= usage.report_date

    qualify row_number() over (
        partition by
            usage.report_date
          , usage.product_code_nk
          , usage.plan_code_nk
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
    , customer_key
    , product_key
    , plan_key
    , currency_key
    , units_used
    , included_units
    , overage_units
    , unit_price
    , billed_value
    , included_value
    , overage_value
    , overage_share
from usage_with_value_metrics