/**
 * profile_usage_daily.sql
 * --------------------
 * Daily profiling metrics for usage:
 * - row counts
 * - volume stats
 * - distinct keys
 *
 * Purpose:
 * - Spot drift over time.
 */

select
      report_date
    , count(*)                                      as row_count
    , sum(units_used)                               as sum_units
    , avg(units_used)                               as avg_units
    , cast(stddev_pop(units_used) as number(18,6))  as stddev_units
    , max(units_used)                               as max_units
    , count(distinct customer_key)                  as distinct_customers
    , count(distinct product_key)                   as distinct_products
    , count(distinct plan_key)                      as distinct_plans
from {{ ref('fact_usage') }}
group by 1
order by 1