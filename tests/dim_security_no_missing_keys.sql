-- Warn if any security_code appears in facts but not in dim_security.

{{ config(severity = 'warn') }}

with

fact_keys as (
    select distinct
        security_code
    from {{ ref('REF_POSITION_ABC_BANK') }}
    where security_code is not null
)

, dim_keys as (
    select
        security_code
    from {{ ref('DIM_SECURITY') }}
)

, base_keys as (
    select
        security_code
    from {{ ref('REF_SECURITY_ABC_BANK') }}
)

, missing as (
    select
        fk.security_code as missing_security_code
      , case
          when bk.security_code is null then 'not_in_base_ref'
          else 'present_in_base_ref'
        end as base_presence
    from fact_keys fk
    left join dim_keys dk
      on dk.security_code = fk.security_code
    left join base_keys bk
      on bk.security_code = fk.security_code
    where dk.security_code is null
)

, counts as (
    select
        count(*) as missing_count
    from missing
)

select
    missing_count
from counts
where missing_count > 0