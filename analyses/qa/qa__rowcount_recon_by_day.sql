{{ config(tags = ['qa']) }}

-- QA Check: Reconcile row counts across layers for a given report_date
-- Param: as_of_date (YYYY-MM-DD, defaults to run_started_at)

{% set as_of_date = var('as_of_date', run_started_at.strftime('%Y-%m-%d')) %}

with stg as (
  select
      count(*) as cnt
  from {{ ref('stg_atlas_meter_usage_daily') }}
  where report_date = to_date('{{ as_of_date }}')
)

, hist as (
  select
      count(*) as cnt
  from {{ ref('hist_atlas_meter_usage_daily') }}
  where report_date = to_date('{{ as_of_date }}')
)

, ref as (
  select
      count(*) as cnt
  from {{ ref('ref_usage_atlas') }}
)

, fact as (
  select
      count(*) as cnt
  from {{ ref('fact_usage') }}
  where report_date = to_date('{{ as_of_date }}')
)

select
    '{{ as_of_date }}' as report_date
  , stg.cnt   as stg_open_cnt
  , hist.cnt  as hist_rows_cnt
  , ref.cnt   as ref_current_cnt
  , fact.cnt  as fact_rows_cnt
from
    stg
  , hist
  , ref
  , fact;
