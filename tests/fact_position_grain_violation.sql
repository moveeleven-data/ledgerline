with

dups as (
  select
        account_key
      , security_key
      , exchange_key
      , currency_key
      , report_date
      , count(*) as row_count
  from {{ ref('fact_position') }}
  group by 1, 2, 3, 4, 5
  having count(*) > 1
)

select * from dups