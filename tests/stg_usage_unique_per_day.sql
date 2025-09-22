with

daily_open_candidate_counts as (
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
from daily_open_candidate_counts
where daily_row_count > 1
