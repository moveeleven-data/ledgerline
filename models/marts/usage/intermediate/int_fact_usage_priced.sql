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
 * Price latest usage at NK grain:
 *   customer_code × product_code × plan_code × report_date.
 *
 * Inputs:
 *   - Usage from REF (ref_usage_atlas)
 *   - Price book from STG for now (stg_atlas_price_book_daily)
 */

with normalized_usage as (
    select
          report_date::date                         as report_date
        , cast(units_used as number(38,0))          as units_used
        , cast(included_units as number(38,0))      as included_units
        , greatest(units_used - included_units, 0)  as overage_units
        , customer_code                             as customer_code_nk
        , product_code                              as product_code_nk
        , plan_code                                 as plan_code_nk
    from {{ ref('ref_usage_atlas') }}
)

, price_book as (
    select
          product_code
        , plan_code
        , price_date
        , unit_price
        , '{{ var("default_billing_currency","USD") }}'::string as currency_code
    from {{ ref('stg_atlas_price_book_daily') }}
)

, usage_priced as (
    select
          normalized_usage_data.report_date
        , normalized_usage_data.customer_code_nk
        , normalized_usage_data.product_code_nk
        , normalized_usage_data.plan_code_nk
        , normalized_usage_data.units_used
        , normalized_usage_data.included_units
        , normalized_usage_data.overage_units
        , price_book_data.unit_price
        , coalesce(
              price_book_data.currency_code
            , '{{ var("default_billing_currency","USD") }}'
          ) as currency_code_nk
    
    from normalized_usage as normalized_usage_data
    left join price_book as price_book_data
           on price_book_data.product_code = normalized_usage_data.product_code_nk
          and price_book_data.plan_code    = normalized_usage_data.plan_code_nk
          and price_book_data.price_date  <= normalized_usage_data.report_date

    qualify row_number() over (
        partition by
             normalized_usage_data.report_date
           , normalized_usage_data.product_code_nk
           , normalized_usage_data.plan_code_nk
        order by
             price_book_data.price_date desc nulls last
    ) = 1
)

select
      report_date
    , customer_code_nk
    , product_code_nk
    , plan_code_nk
    , units_used
    , included_units
    , overage_units
    , unit_price
    , currency_code_nk
from usage_priced
