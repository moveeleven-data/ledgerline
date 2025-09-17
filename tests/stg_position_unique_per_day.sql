with

daily_open_candidate_counts as (
    select
        account_code
      , security_code
      , report_date
      , count(*) as daily_row_count
    from {{ ref('STG_ABC_BANK_POSITION') }}
    group by
        account_code
      , security_code
      , report_date
)
select *
from daily_open_candidate_counts
where daily_row_count > 1
