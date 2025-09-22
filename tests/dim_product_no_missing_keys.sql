{{ config(severity = 'warn') }}

-- Warn if any product_code appears in facts but NOT in DIM_PRODUCT

with

fact_codes as (
    select distinct
        product_code
    from {{ ref('ref_usage_atlas') }}
    where product_code is not null
)

, dim_codes as (
    select
        product_code
    from {{ ref('dim_product') }}
)

select
    fact_codes.product_code
from fact_codes
left join dim_codes using (product_code)
where dim_codes.product_code is null
