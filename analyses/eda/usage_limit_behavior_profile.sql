/**
eda__usage_limit_behavior_profile.sql
-------------------------------------
Summarize subscription usage and overages from the past 90 days to help guide plan recommendations.

Grain:
- One row per customer–product–plan over a 90-day window

Approach:
- Track activity days, overage days, streaks, utilization, and effective unit price
- Only include subscriptions with enough meaningful activity

Downstream Usage
Figures:
- fig_overage_distribution_cdf.png:  Shows overage patterns by product
- fig_limit_streaks_by_plan.png:     Shows longest overage streaks by plan
- fig_fairness_by_country.png:       Compares usage fairness across countries
*/

with

subscription_active_day_counts as (
    select
          customer_key
        , product_key
        , plan_key
        , count(*) as active_days 
    from {{ ref('fact_usage_window') }}
    group by
          customer_key
        , product_key
        , plan_key
)

, subscription_overage_day_counts as (
    select
          customer_key
        , product_key
        , plan_key
        , sum(case when overage_units > 0 then 1 else 0 end) as days_over_limit
    from {{ ref('fact_usage_window') }}
    group by
          customer_key
        , product_key
        , plan_key
)

, subscription_day_counts_joined as (
    select
          customer_key
        , product_key
        , plan_key
        , active_days 
        , days_over_limit
    from subscription_active_day_counts
    inner join subscription_overage_day_counts
        using (customer_key, product_key, plan_key)
)

, subscription_limit_hit_ratios as (
    select
        *
      , round(
            days_over_limit::number / active_days
          , 2
        ) as limit_hit_ratio
    from subscription_day_counts_joined
)

, subscription_utilization as (
    select
          customer_key
        , product_key
        , plan_key
        , round(
              avg(
                  units_used::number
                  / nullif(included_units, 0)
              )
            , 2
          )
          ) as utilization
    from {{ ref('fact_usage_window') }}
    group by
          customer_key
        , product_key
        , plan_key
)

select
    *
from subscription_utilization