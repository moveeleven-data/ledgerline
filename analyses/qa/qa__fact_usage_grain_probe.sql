{{ config(tags = ['qa']) }}

/**
 * qa__fact_usage_grain_probe.sql
 * ------------------------------
 * Detect duplicate rows in Usage Fact table at the declared grain.
 *
 * Grain:
 * - (customer_key, product_key, plan_key, report_date)
 *
 * Input:
 * - as_of_date (optional). If provided, restricts the check to that date.
 */

{% set as_of_date = var('as_of_date', none) %}

with

fact_usage_filtered as (
    select
        *
    from {{ ref('fact_usage') }}

    {% if as_of_date %}
    where
        report_date = to_date('{{ as_of_date }}')
    {% endif %}
)

select
    customer_key
  , product_key
  , plan_key
  , report_date
  , count(*) as row_count
from fact_usage_filtered
group by
    customer_key
  , product_key
  , plan_key
  , report_date
having
    count(*) > 1
order by
    row_count desc;
