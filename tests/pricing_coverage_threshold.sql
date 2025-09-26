/**
 * pricing_coverage_threshold.sql
 * -----------------------------
 * Companion test to pricing_missing_rows.
 *
 * Together they validate pricing coverage:
 * - This test (error) checks coverage rate: fails if <95% of usage rows
 *   in the last 7 days have a unit_price.
 * - pricing_missing_rows.sql (warn) lists the specific rows that are unpriced.
 *
 * Purpose:
 * Ensures billing integrity by catching large-scale gaps in pricing.
 */

{{ config(tags=['qa'], severity='error') }}

{% set as_of_str  = get_latest_usage_report_date() %}
{% set start_date = "dateadd(day, -6, to_date('" ~ as_of_str ~ "'))" %}
{% set end_date   = "to_date('" ~ as_of_str ~ "')" %}

with

usage_window as (
    select
        report_date
      , unit_price
    from {{ ref('fact_usage') }}
    where
        report_date between {{ start_date }} and {{ end_date }}
)

, pricing_counts as (
    select
         count(*) as total_usage_rows
       , sum(
            case 
                when unit_price is not null then 1 else 0
            end
        ) as priced_usage_rows
    from usage_window
)

select
    *
from pricing_counts
where
    priced_usage_rows::float / nullif(total_usage_rows, 0) < 0.95