/**
 * qa__default_hits_by_day.sql
 * ---------------------------
 * Check fact rows against dimension tables.
 *
 * Purpose:
 * - Count usage rows that fail to join to dim_customer, dim_product, or dim_plan.
 * - Count usage rows that join, but land on the default product row (-1).
 *
 * Input:
 * - as_of_date: reporting date (YYYY-MM-DD).
 *   Defaults to run_started_at if not provided.
 *
 * Output:
 * - One row for the given date with counts of missing or default matches.
 */

{% set as_of_date = var('as_of_date', run_started_at.strftime('%Y-%m-%d')) %}

select
    fact_usage.report_date

  , count_if(dim_customer.customer_key is null) as missing_customer_dim
  , count_if(dim_product.product_key is null)   as missing_product_dim
  , count_if(dim_plan.plan_key is null)          as missing_plan_dim

  , count_if(
            dim_product.product_key is not null
        and dim_product.product_code = '-1'
    ) as hits_product_default

from {{ ref('fact_daily_usage') }} as fact_usage

left join {{ ref('dim_customer') }} as dim_customer
       on dim_customer.customer_key = fact_usage.customer_key

left join {{ ref('dim_product') }} as dim_product
       on dim_product.product_key = fact_usage.product_key

left join {{ ref('dim_plan') }} as dim_plan
       on dim_plan.plan_key = fact_usage.plan_key

where
    fact_usage.report_date = to_date('{{ as_of_date }}')
group by
    fact_usage.report_date;