/**
billed_amount_price_sensitivity.sql
----------------------------------------
Purpose:
- Measure sensitivity of billed amounts to price shifts.
- Flag products and plans as stable, moderate, or volatile
- Compare actual spend with what-if scenarios

Grain:
- One row per product–plan, aggregated over 90 days

Downstream Usage
- fig_price_sensitivity_by_product.png: Shows products grouped by plan with stability flagged
- Report: Includes a note on whether price changes may bias recommendations
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
        end as price_volatility
      , round(
            sum(billed_value)
          , 2
        ) as total_billed_value
    from {{ ref('fact_usage_window') }}
    group by
        product_key
      , plan_key
)

, price_sensitivity as (
    select
        product_name
      , plan_name
      , distinct_unit_prices
      , price_volatility
      , total_billed_value
    from window_price_stats
    inner join {{ ref('dim_product') }}
        using (product_key)
    inner join {{ ref('dim_plan') }}
        using (plan_key)
)

select
    product_name
  , plan_name
  , distinct_unit_prices
  , price_volatility
  , total_billed_value
from price_sensitivity