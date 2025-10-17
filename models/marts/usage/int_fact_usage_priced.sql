{{ config(
     materialized = 'view'
   , tags = [
         'mart:usage'
       , 'intermediate'
       , 'domain:usage_billing'
     ]
) }}

/**
 * int_fact_usage_priced.sql
 * -------------------------
 * Price latest usage at NK grain.
 * - Keys for customer/product/plan flow from REF usage (created in STG).
 * - Currency appears here for the first time; compute currency_key now.
 * - Also compute billed/included/overage values so the FACT can be a pure SELECT.
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
          usage.report_date
        , usage.customer_key
        , usage.product_key
        , usage.plan_key
        , usage.units_used
        , usage.included_units
        , usage.overage_units

        , coalesce(price.unit_price, 0)                as unit_price
        , price.currency_code_nk                       as currency_code_nk
        , {{ dbt_utils.generate_surrogate_key(["upper(currency_code_nk)"]) }} as currency_key
    
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