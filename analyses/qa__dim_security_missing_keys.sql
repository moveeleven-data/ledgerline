-- List security_codes that appear in facts but do not exist in dim_security.
-- This shows where the self-completing dimension had to synthesize a row.

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

select
    fk.security_code as missing_security_code
  , case
      when bk.security_code is null then 'not_in_base_ref'  -- missing in source
      else 'present_in_base_ref'                            -- present in source
    end as base_presence
from fact_keys fk
left join dim_keys dk
  on dk.security_code = fk.security_code
left join base_keys bk
  on bk.security_code = fk.security_code
where dk.security_code is null   -- keys that forced self-completion
order by
    missing_security_code
