/**
 * pricing_missing_rows.sql
 * ------------------------
 * Companion test to pricing_coverage_threshold.sql.sql.
 *
 * Together they validate pricing coverage:
 * - pricing_coverage_threshold.sql.sql (ERROR) fails if coverage <95%.
 * - This test (WARN) surfaces the exact customer/product/plan/date rows
 *   that are missing a price.
 *
 * Purpose:
 * Helps pinpoint the root cause of revenue leakage when the coverage test fails.
 */

{{ config(tags = ['qa'], severity = 'warn') }}

with

usage_rows as (
  select
      upper(customer_code)   as customer_code
    , upper(product_code)    as product_code
    , upper(plan_code)       as plan_code
    , report_date::date      as report_date
  from {{ ref('ref_usage_atlas') }}
)

, price_rows as (
  select
      upper(product_code)    as product_code
    , upper(plan_code)       as plan_code
    , price_date::date       as price_date
    , unit_price
  from {{ ref('stg_atlas_price_book_daily') }}
)

, usage_without_price as (
  select
      usage_rows.product_code
    , usage_rows.plan_code
    , usage_rows.report_date
  from usage_rows
  left join price_rows
    on  price_rows.product_code = usage_rows.product_code
    and price_rows.plan_code    = usage_rows.plan_code
    and price_rows.price_date   = usage_rows.report_date
  where price_rows.unit_price is null
)

select
    *
from usage_without_price