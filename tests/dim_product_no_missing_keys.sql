{{ config(severity = 'warn') }}

-- Warn if any product_code appears in facts but NOT in DIM_PRODUCT

with

fact_codes as (
    select distinct
        product_code
    from {{ ref('REF_USAGE_ATLAS') }}
    where product_code is not null
)

, dim_codes as (
    select
        product_code
    from {{ ref('DIM_PRODUCT') }}
)

select
    fact_codes.product_code
from fact_codes
left join dim_codes using (product_code)
where dim_codes.product_code is null
