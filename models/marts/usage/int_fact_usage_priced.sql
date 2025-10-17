{{ config(materialized='view') }}

/**
 * int_fact_usage_priced.sql
 * -------------------------
 * Price latest usage at NK grain.
 * - Compute billed/included/overage values so the FACT can be a pure SELECT.
 */

with

-- Normalize base usage
normalized_usage as (
    select
          report_date::date                        as report_date
        , cast(units_used as number(38, 0))        as units_used
        , cast(included_units as number(38, 0))    as included_units
        , greatest(units_used - included_units, 0) as overage_units
        , customer_hkey                            as customer_key
        , product_hkey                             as product_key
        , plan_hkey                                as plan_key
        , product_code                             as product_code_nk
        , plan_code                                as plan_code_nk
    from {{ ref('ref_usage_atlas') }}
)

-- Bring in pricing and currency
, usage_with_price as (
    select
          u.report_date
        , u.customer_key
        , u.product_key
        , u.plan_key
        , u.units_used
        , u.included_units
        , u.overage_units
        , coalesce(p.unit_price, 0) as unit_price
        , p.currency_key as currency_key

    from normalized_usage as u
    left join {{ ref('ref_price_book_daily') }} as p
           on p.product_code = u.product_code_nk
          and p.plan_code    = u.plan_code_nk
          and p.price_date  <= u.report_date

    qualify row_number() over (
        partition by
            u.report_date
          , u.product_code_nk
          , u.plan_code_nk
        order by
            p.price_date desc nulls last
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
