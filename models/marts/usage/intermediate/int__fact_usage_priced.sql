{{ config(
     materialized = 'view'
   , tags = ['mart:usage', 'intermediate', 'domain:usage_billing']
) }}

/**
 * int__fact_usage_priced.sql
 * --------------------------
 * Intermediate model: prepare priced usage at the grain
 *   customer_code × product_code × plan_code × report_date.
 *
 * Responsibilities:
 * - Normalize raw usage rows and compute overage_units.
 * - Look up the last effective unit_price on or before report_date.
 * - Assign an explicit currency_code during pricing.
 *
 * Notes:
 * - This model isolates pricing rules so fact_usage stays thin.
 * - Easier to test pricing logic separately and trace lineage.
 */

with

/* Step 1. Normalize usage rows.
   - Coerce numeric types and compute overage_units.
   - Coalesce natural keys to '-1' as a safe default. */

normalized_usage as (
    select
          report_date::date                         as report_date
        , cast(units_used as number(38,0))          as units_used
        , cast(included_units as number(38,0))      as included_units
        , greatest(units_used - included_units, 0)  as overage_units
        , coalesce(customer_code, '-1')             as customer_code_nk
        , coalesce(product_code,  '-1')             as product_code_nk
        , coalesce(plan_code,     '-1')             as plan_code_nk
    from {{ ref('ref_usage_atlas') }}
)


/* Step 2. Prepare price book.
   - Bring in daily unit prices.
   - Ensure a deterministic currency_code (project var with default 'USD'). */

, price_book as (
    select
          price_src.product_code
        , price_src.plan_code
        , price_src.price_date
        , price_src.unit_price
        , '{{ var("default_billing_currency","USD") }}'::string as currency_code
    from {{ ref('stg_atlas_price_book_daily') }} as price_src
)


/* Step 3. Pick the last effective price on or before report_date.
   - Join usage to prices with price_date <= report_date.
   - Rank within each usage day and product-plan, keep the most recent row. */

, usage_priced as (
    select
          u.report_date
        , u.customer_code_nk
        , u.product_code_nk
        , u.plan_code_nk
        , u.units_used
        , u.included_units
        , u.overage_units
        , p.unit_price
        , coalesce(p.currency_code, '{{ var("default_billing_currency","USD") }}') as currency_code_nk
    from normalized_usage as u
    left join price_book as p
           on p.product_code = u.product_code_nk
          and p.plan_code    = u.plan_code_nk
          and p.price_date  <= u.report_date

    qualify
        row_number() over (
            partition by
                 u.report_date
               , u.product_code_nk
               , u.plan_code_nk
            order by
                 p.price_date desc nulls last
    ) = 1
)

select
    *
from usage_priced
