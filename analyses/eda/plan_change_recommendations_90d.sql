/**
plan_change_recommendations_90d.sql
-----------------------------------
Simple 90 day rollup and a single recommendation.

Signals
- overage_rate    = overage_days / days_observed
- overage_share   = overage_value_90d / billed_value_90d
- avg_utilization = units_used_90d / included_units_90d

Recommendation priority
upsell > adjust units > down tier > hold
*/

with

rollup_90d as (
    select
          dim_customer.customer_name  as customer_name
        , fact.product_name           as product_name
        , fact.plan_name              as plan_name

        , count(*) as days_observed
        , sum(
              case
                  when fact.overage_units > 0 then 1
                  else 0
              end
          ) as overage_days

        , sum(fact.units_used)      as units_used_90d
        , sum(fact.included_units)  as included_units_90d

        , sum(fact.overage_value)   as overage_value_90d
        , sum(fact.billed_value)    as billed_value_90d
    from {{ ref('fact_usage_window') }} as fact
    join {{ ref('dim_customer') }}      as dim_customer
        using (customer_key)
    group by
          dim_customer.customer_name
        , fact.product_name
        , fact.plan_name
)

, metrics as (
    select
          customer_name
        , product_name
        , plan_name
        , days_observed

        , round(
                  overage_days::decimal
                / nullif(days_observed, 0)
              , 2
          ) as overage_rate

        , round(
                  units_used_90d::decimal
                / nullif(included_units_90d, 0)
              , 2
          ) as avg_utilization

        , round(
                case
                    when billed_value_90d > 0
                    then overage_value_90d::decimal
                       / billed_value_90d
                    else 0
                end
              , 2
          ) as overage_share
    from rollup_90d
)

, recommendations as (
    select
          customer_name
        , product_name
        , plan_name
        , days_observed
        , overage_rate
        , overage_share
        , avg_utilization

        , case
              when days_observed < 5 then 'hold'
              when overage_share >= 0.30
                or overage_rate  >= 0.50
                  then 'upsell'
              when overage_share >= 0.10
                or overage_rate  >= 0.10
                  then 'adjust units'
              when overage_rate = 0
               and avg_utilization <= 0.50
                  then 'down tier'
              else 'hold'
          end as recommendation
    from metrics
)

, prioritized as (
    select
          customer_name
        , product_name
        , plan_name
        , days_observed
        , overage_rate
        , overage_share
        , avg_utilization
        , recommendation

        , case recommendation
              when 'upsell'        then 1
              when 'adjust units'  then 2
              when 'down tier'     then 3
              else                      4
          end as recommendation_priority
    from recommendations
)

select
      customer_name
    , product_name
    , plan_name
    , days_observed
    , overage_rate
    , overage_share
    , avg_utilization
    , recommendation
from prioritized
order by
      recommendation_priority
    , customer_name
    , product_name
    , plan_name