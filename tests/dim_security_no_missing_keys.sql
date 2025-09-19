{{ config(severity = 'warn') }}

-- Warn if any security_code appears in facts but NOT in DIM_SECURITY

with

fact_codes as (
    select distinct security_code
    from {{ ref('REF_POSITION_ABC_BANK') }}
    where security_code is not null
)

, dim_codes as (
    select security_code
    from {{ ref('DIM_SECURITY') }}
)

select fact_codes.security_code
from fact_codes
left join dim_codes using (security_code)
where dim_codes.security_code is null