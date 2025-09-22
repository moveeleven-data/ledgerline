{{ config(tags = ['qa']) }}

-- QA Check: Foreign key coverage and default-member usage for FACT_USAGE
-- Inputs:
--   as_of_date: target reporting date (YYYY-MM-DD, defaults to run_started_at)

{% set as_of_date = var('as_of_date', run_started_at.strftime('%Y-%m-%d')) %}

select
    f.report_date
  , count_if(c.customer_key is null)                                 as missing_customer_dim
  , count_if(p.product_key  is null)                                 as missing_product_dim
  , count_if(pl.plan_key    is null)                                 as missing_plan_dim
  , count_if(p.product_key  is not null and p.product_code = '-1')   as hits_product_default
from {{ ref('fact_usage') }} f
left join {{ ref('dim_customer') }} c on c.customer_key = f.customer_key
left join {{ ref('dim_product')  }} p on p.product_key  = f.product_key
left join {{ ref('dim_plan')     }} pl on pl.plan_key   = f.plan_key
where f.report_date = to_date('{{ as_of_date }}')
group by
    f.report_date;
