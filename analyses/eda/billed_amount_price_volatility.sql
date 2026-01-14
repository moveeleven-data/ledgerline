/**
billed_amount_price_volatility.sql
----------------------------------------
Purpose:
- Measure volatility of billed amounts to price shifts.
- Flag products and plans as stable, moderate, or volatile

Grain:
- One row per product–plan, aggregated over 90 days
*/

with

window_price_stats as (

    select
          product_key
        , plan_key
        , count(distinct unit_price) as distinct_unit_prices
        , case
              when count(distinct unit_price) > 3 then 'volatile'
              when count(distinct unit_price) > 1 then 'moderate'
              else 'stable'
          end as volatility_level
        , round(sum(billed_value), 2) as total_billed_value
    from {{ ref('fact_daily_usage') }}
    where report_date >= dateadd(day, -90, current_date)
    group by
          product_key
        , plan_key
)

select
      window_price_stats.product_key
    , dim_product.product_name
    , window_price_stats.plan_key
    , dim_plan.plan_name
    , window_price_stats.distinct_unit_prices
    , window_price_stats.volatility_level
    , window_price_stats.total_billed_value
from window_price_stats
join {{ ref('dim_product') }} as dim_product
    on dim_product.product_key = window_price_stats.product_key
join {{ ref('dim_plan') }} as dim_plan
    on dim_plan.plan_key = window_price_stats.plan_key