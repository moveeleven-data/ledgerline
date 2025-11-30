{{ config(materialized='view') }}

/**
 * usage_anomalies_daily.sql
 * -------------------------
 * Flag days where usage volume looks suspicious vs history.
 * - Today’s row_count is > 3 stddevs away from the historical mean.
 */

with

usage_rowcount_baseline as (
    select
          cast(avg(row_count)        as number(38,6)) as avg_daily_row_count
        , cast(stddev_pop(row_count) as number(38,6)) as stddev_daily_row_count
    from {{ ref('profile_usage_daily') }}
)

, usage_rowcount_flagged as (
    select
          usage_profile.report_date
        , usage_profile.row_count
        , usage_rowcount_baseline.avg_daily_row_count
        , usage_rowcount_baseline.stddev_daily_row_count
        , coalesce(
              abs(
                  usage_profile.row_count
                - usage_rowcount_baseline.avg_daily_row_count
              ) > 3 * usage_rowcount_baseline.stddev_daily_row_count
            , false
          ) as is_row_count_outlier
    from {{ ref('profile_usage_daily') }} as usage_profile
    cross join usage_rowcount_baseline
)

select
      report_date
    , row_count
    , avg_daily_row_count
    , stddev_daily_row_count
    , is_row_count_outlier
from usage_rowcount_flagged
where is_row_count_outlier