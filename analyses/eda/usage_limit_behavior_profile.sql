/**
usage_limit_behavior_profile.sql
------------------------------------
Summarize subscription usage and overages from the past 90 days to help guide plan recommendations.

Grain:
- one row per customer_key × product_key × plan_key

Start date:
- window_start_date = 2025-08-30, window_end_date = 2025-09-28
*/

with

subscriptions_daily as (
    select
          customer_key
        , product_key
        , plan_key
        , report_date
        , max(units_used)     as units_used
        , max(included_units) as included_units
        , max(overage_units)  as overage_units
    from {{ ref('fact_usage_window') }}
    group by
          customer_key
        , product_key
        , plan_key
        , report_date
)

, day_counts as (
    select
          customer_key
        , product_key
        , plan_key
        , count(*) as active_days
        , count_if(overage_units > 0) as days_over_limit
        , round(
              count_if(overage_units > 0)::number
              / nullif(count(*), 0)
            , 2
          ) as limit_hit_ratio
    from subscriptions_daily
    group by
          customer_key
        , product_key
        , plan_key
)

, utilization_by_subscription as (
    select
          customer_key
        , product_key
        , plan_key
        , round(
              avg(
                  case when included_units <> 0
                       then units_used::number / included_units
                  end
              )
            , 2
          ) as utilization
    from subscriptions_daily
    group by
          customer_key
        , product_key
        , plan_key
)

, overage_blocks as (
    select
          customer_key
        , product_key
        , plan_key
        , report_date
        , (overage_units > 0) as is_over
        , count_if(overage_units = 0) over (
              partition by
                  customer_key
                , product_key
                , plan_key
              order by
                  report_date
              rows between
                  unbounded preceding and current row
          ) as block_id
    from subscriptions_daily
)

, streaks as (
    select
          customer_key
        , product_key
        , plan_key
        , coalesce(
              max(block_length)
            , 0
          ) as streak_days_over_limit
    from (
        select
              customer_key
            , product_key
            , plan_key
            , block_id
            , count(*) over (
                  partition by
                       customer_key
                     , product_key
                     , plan_key
                     , block_id
              ) as block_length
        from overage_blocks
        where is_over = true
    ) as block_lengths
    group by
          customer_key
        , product_key
        , plan_key
)

, window_totals as (
    select
          customer_key
        , product_key
        , plan_key
        , sum(units_used)     as units_used_90d
        , sum(included_units) as included_units_90d
        , sum(overage_units)  as overage_units_90d
        , min(report_date)    as window_start_date
        , max(report_date)    as window_end_date
    from subscriptions_daily
    group by
          customer_key
        , product_key
        , plan_key
)

, subscription_usage_profile as (
    select
          day_counts.customer_key
        , day_counts.product_key
        , day_counts.plan_key
        , day_counts.active_days
        , day_counts.days_over_limit
        , day_counts.limit_hit_ratio
        , util.utilization as utilization
        , coalesce(streaks.streak_days_over_limit, 0) as streak_days_over_limit
        , totals.units_used_90d
        , totals.included_units_90d
        , totals.overage_units_90d
        , totals.window_start_date
        , totals.window_end_date
    from day_counts
    left join utilization_by_subscription util
         on day_counts.customer_key = util.customer_key
        and day_counts.product_key  = util.product_key
        and day_counts.plan_key     = util.plan_key
    left join streaks
         on day_counts.customer_key = streaks.customer_key
        and day_counts.product_key  = streaks.product_key
        and day_counts.plan_key     = streaks.plan_key
    left join window_totals totals
         on day_counts.customer_key = totals.customer_key
        and day_counts.product_key  = totals.product_key
        and day_counts.plan_key     = totals.plan_key
    where
        day_counts.active_days >= 30
)

select
    plan_key
  , customer_key
  , streak_days_over_limit
from subscription_usage_profile
order by
    plan_key
  , customer_key