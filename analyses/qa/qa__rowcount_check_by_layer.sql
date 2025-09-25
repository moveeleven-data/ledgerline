{{ config(tags = ['qa']) }}

/**
 * qa__rowcount_check_by_layer.sql
 * -------------------------------
 * Check row counts across pipeline layers for one reporting date.
 *
 * Purpose:
 * - Compare how many rows exist in staging, history, refined, and fact.
 * - Helps catch data loss or duplication as rows move through the pipeline.
 *
 * Output:
 * - A single row with counts from each layer.
 */

{% set as_of_date = var('as_of_date', run_started_at.strftime('%Y-%m-%d')) %}

with

staging_rowcount as (
    select count(*) as staging_count
    from {{ ref('stg_atlas_meter_usage_daily') }}
    where report_date = to_date('{{ as_of_date }}')
)


, history_rowcount as (
    select count(*) as history_count
    from {{ ref('hist_atlas_meter_usage_daily') }}
    where report_date = to_date('{{ as_of_date }}')
)


, refined_rowcount as (
    select count(*) as refined_count
    from {{ ref('ref_usage_atlas') }}
)


, fact_rowcount as (
    select count(*) as fact_count
    from {{ ref('fact_usage') }}
    where report_date = to_date('{{ as_of_date }}')
)


select
    '{{ as_of_date }}'               as report_date
  , staging_rowcount.staging_count   as staging_rows
  , history_rowcount.history_count   as history_rows
  , refined_rowcount.refined_count   as refined_rows
  , fact_rowcount.fact_count         as fact_rows
from
    staging_rowcount
  , history_rowcount
  , refined_rowcount
  , fact_rowcount;
