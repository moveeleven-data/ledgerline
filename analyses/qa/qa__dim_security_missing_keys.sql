{{ config(tags=['qa']) }}

-- QA Check: Identify fact-level security_codes absent from the base seed/ref
-- Context: These codes triggered self-completion in DIM_SECURITY

with fact_codes as (
  select distinct security_code
  from {{ ref('REF_POSITION_ABC_BANK') }}
  where security_code is not null
)

, base_codes as (
  select security_code
  from {{ ref('REF_SECURITY_ABC_BANK') }}
)

, dim_codes as (
  select security_code
  from {{ ref('DIM_SECURITY') }}
)

select
    f.security_code
  , case
        when b.security_code is null then 'not_in_base_ref'
        else 'present_in_base_ref'
    end as base_presence

  , case when d.security_code is null then 'missing_in_dim'
         else 'present_in_dim'
    end as dim_presence

from fact_codes f
left join base_codes b on b.security_code = f.security_code
left join dim_codes  d on d.security_code = f.security_code
where b.security_code is null
order by f.security_code
