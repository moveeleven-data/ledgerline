{{ config(tags=['qa']) }}

-- QA Check: Detect duplicate rows at FACT_POSITION grain
-- Grain: (account_key, security_key, exchange_key, currency_key, report_date)
-- Param: as_of_date (optional filter to narrow scope)

{% set as_of_date = var('as_of_date', none) %}

with scoped as (
  select *
  from {{ ref('FACT_POSITION') }}
  {% if as_of_date %}
  where report_date = to_date('{{ as_of_date }}')
  {% endif %}
)

select
    account_key
  , security_key
  , exchange_key
  , currency_key
  , report_date
  , count(*) as row_count
from scoped
group by 1,2,3,4,5
having count(*) > 1
order by row_count desc
