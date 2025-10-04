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

usage_by_country_plan as (
    select
          country_name
        , plan_name
        , count(distinct customer_key) as customers_active
        , round(
              sum(units_used) 
              / nullif(
                   sum(included_units)
                 , 0)::numeric
            , 2
          ) as utilization_avg
        , round(
              sum(billed_value) 
              / nullif(
                   sum(units_used)
                 , 0)::numeric
            , 6
          ) as effective_unit_price_avg
    from {{ ref('fact_usage_window') }}
    join {{ ref('dim_customer') }}
        using (customer_key)
    join {{ ref('dim_country') }}
        using (country_code)
    group by
          country_name
        , plan_name
)

, daily_overages as (
    select
          country_name
        , plan_name
        , report_date
        , case 
              when sum(overage_units) > 0 then 1 
              else 0 
          end as has_overage
    from {{ ref('fact_usage_window') }}
    join {{ ref('dim_customer') }}
        using (customer_key)
    join {{ ref('dim_country') }}
        using (country_code)
    group by
          country_name
        , plan_name
        , report_date
)

, over_limit_rate as (
    select
          country_name
        , plan_name
        , round(
             avg(has_overage)
           , 2
          ) as days_over_limit_rate
    from daily_overages
    group by country_name, plan_name
)

, country_plan as (
    select
          usage_by_country_plan.country_name
        , usage_by_country_plan.plan_name
        , usage_by_country_plan.customers_active
        , usage_by_country_plan.utilization_avg
        , usage_by_country_plan.effective_unit_price_avg
        , over_limit_rate.days_over_limit_rate
    from usage_by_country_plan
    join over_limit_rate
        using (country_name, plan_name)
)

, plan_benchmark as (
    select
          plan_name
        , sum(customers_active) as plan_customers_active
        , round(
              sum(days_over_limit_rate * customers_active) 
              / nullif(sum(customers_active), 0)
            , 2
          ) as plan_days_over_limit_rate
    from country_plan
    group by plan_name
)

, fairness_by_country_and_plan as (
    select
        country_name
        , plan_name
        , customers_active
        , utilization_avg
        , effective_unit_price_avg
        , days_over_limit_rate
        , plan_days_over_limit_rate
        , (days_over_limit_rate - plan_days_over_limit_rate) as delta
        , case
            when customers_active >= 2
            and days_over_limit_rate > plan_days_over_limit_rate + 0.10
            then true
            else false
        end as fairness_flag
        , case
            when customers_active < 2 then 'small_sample'
            when days_over_limit_rate >= plan_days_over_limit_rate + 0.10 then 'alert'
            when days_over_limit_rate >= plan_days_over_limit_rate + 0.05 then 'warn'
            else 'ok'
        end as severity
    from country_plan
    inner join plan_benchmark
        using (plan_name)
)

select
    *
from fairness_by_country_and_plan