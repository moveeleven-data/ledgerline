{{ config(tags = ['qa']) }}

-- QA Check: Identify fact-level product_codes absent from the base ref
-- Context: These codes triggered self-completion in DIM_PRODUCT

with fact_codes as (
  select distinct
      product_code
  from {{ ref('REF_USAGE_ATLAS') }}
  where product_code is not null
)

, base_codes as (
  select
      product_code
  from {{ ref('REF_PRODUCT_ATLAS') }}
)

, dim_codes as (
  select
      product_code
  from {{ ref('DIM_PRODUCT') }}
)

select
    f.product_code
  , case
        when b.product_code is null then 'not_in_base_ref'
        else 'present_in_base_ref'
    end as base_presence
  , case
        when d.product_code is null then 'missing_in_dim'
        else 'present_in_dim'
    end as dim_presence
from fact_codes f
left join base_codes b on b.product_code = f.product_code
left join dim_codes  d on d.product_code = f.product_code
where b.product_code is null
order by
    f.product_code;
