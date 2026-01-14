/**
fairness_by_country_and_plan.sql
-------------------------------------
Assess fairness across countries and plans by comparing usage patterns
and unit prices from the past 90 days.

Purpose:
- Spot country–plan combinations that look uneven
- Account for utilization and volume when comparing outcomes

Approach:
- Track how many customers are active and how they use their plans
- Compare each country–plan against plan-level averages
- Flag cases that look uneven and have enough data to be reliable

Grain:
- One row per country–plan over a 90-day window
*/

with

usage_90d as (

    select
          customer_key
        , plan_key
        , report_date
        , units_used
        , included_units
        , overage_units
        , billed_value
    from {{ ref('fact_daily_usage') }}
    where report_date >= dateadd(day, -90, current_date)

)

, usage_enriched as (

    select
          usage_90d.customer_key
        , usage_90d.plan_key
        , usage_90d.report_date
        , usage_90d.units_used
        , usage_90d.included_units
        , usage_90d.overage_units
        , usage_90d.billed_value
        , dim_customer.country_code
        , dim_country.country_key
    from usage_90d
    join {{ ref('dim_customer') }} as dim_customer
        on dim_customer.customer_key = usage_90d.customer_key
    join {{ ref('dim_country') }} as dim_country
        on dim_country.country_code = dim_customer.country_code

)

, usage_by_country_plan as (

    select
          country_key
        , plan_key
        , count(distinct customer_key) as customers_active
        , round(
              sum(units_used)
              / nullif(sum(included_units), 0)::numeric
            , 2
          ) as utilization_avg
        , round(
              sum(billed_value)
              / nullif(sum(units_used), 0)::numeric
            , 6
          ) as effective_unit_price_avg
    from usage_enriched
    group by
          country_key
        , plan_key

)

, daily_overages as (

    select
          country_key
        , plan_key
        , report_date
        , case
              when sum(overage_units) > 0 then 1
              else 0
          end as has_overage
    from usage_enriched
    group by
          country_key
        , plan_key
        , report_date

)

, over_limit_rate as (

    select
          country_key
        , plan_key
        , round(avg(has_overage), 2) as days_over_limit_rate
    from daily_overages
    group by
          country_key
        , plan_key

)

, country_plan as (

    select
          usage_by_country_plan.country_key
        , usage_by_country_plan.plan_key
        , usage_by_country_plan.customers_active
        , usage_by_country_plan.utilization_avg
        , usage_by_country_plan.effective_unit_price_avg
        , over_limit_rate.days_over_limit_rate
    from usage_by_country_plan
    join over_limit_rate
        on over_limit_rate.country_key = usage_by_country_plan.country_key
       and over_limit_rate.plan_key    = usage_by_country_plan.plan_key

)

, plan_benchmark as (

    select
          plan_key
        , sum(customers_active) as plan_customers_active
        , round(
              sum(days_over_limit_rate * customers_active)
              / nullif(sum(customers_active), 0)
            , 2
          ) as plan_days_over_limit_rate
    from country_plan
    group by plan_key

)

, fairness_by_country_and_plan as (

    select
          country_plan.country_key
        , country_plan.plan_key
        , country_plan.customers_active
        , country_plan.utilization_avg
        , country_plan.effective_unit_price_avg
        , country_plan.days_over_limit_rate
        , plan_benchmark.plan_days_over_limit_rate
        , (country_plan.days_over_limit_rate - plan_benchmark.plan_days_over_limit_rate) as delta

        , case
              when country_plan.customers_active >= 2
               and country_plan.days_over_limit_rate > plan_benchmark.plan_days_over_limit_rate + 0.10
              then true
              else false
          end as fairness_flag

        , case
              when country_plan.customers_active < 2 then 'small_sample'
              when country_plan.days_over_limit_rate >= plan_benchmark.plan_days_over_limit_rate + 0.10 then 'alert'
              when country_plan.days_over_limit_rate >= plan_benchmark.plan_days_over_limit_rate + 0.05 then 'warn'
              else 'ok'
          end as severity
          
    from country_plan
    join plan_benchmark
        on plan_benchmark.plan_key = country_plan.plan_key

)

select
    *
from fairness_by_country_and_plan