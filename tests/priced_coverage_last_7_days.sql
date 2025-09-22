{{ config(severity='error') }}

{% set as_of_str = get_latest_usage_report_date() %}
{% set start_expr = "dateadd(day, -6, to_date('" ~ as_of_str ~ "'))" %}
{% set end_expr   = "to_date('" ~ as_of_str ~ "')" %}

with window_bounds as (
  select
      {{ start_expr }} as start_date
    , {{ end_expr }}   as end_date
)

, scoped as (
  select
        report_date
      , unit_price
  from {{ ref('fact_usage') }}
  where report_date between (select start_date from window_bounds)
                        and (select end_date   from window_bounds)
)

, stats as (
  select
        count(*) as total_rows
      , sum(
            case
                when unit_price is not null then 1
                else 0
            end
        ) as priced_rows
  from scoped
)

select *
from stats
where priced_rows::float / nullif(total_rows, 0) < 0.95