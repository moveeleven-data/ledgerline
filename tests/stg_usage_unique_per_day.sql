/**
 * stg_usage_unique_per_day.sql
 * ----------------------------
 * Errors if multiple rows exist per (customer, product, plan, date).
 *
 * Purpose:
 * This is the core staging fact for the star schema. It must be unique
 * at the daily grain, or downstream facts and dimensions will break.
 */

{{ config(severity = 'error') }}

with usage_daily_counts as (
    select
          customer_code
        , product_code
        , plan_code
        , report_date
        , count(*) as daily_row_count
    from {{ ref('stg_atlas_meter_usage_daily') }}
    group by
          customer_code
        , product_code
        , plan_code
        , report_date
)

select *
from usage_daily_counts
where daily_row_count > 1