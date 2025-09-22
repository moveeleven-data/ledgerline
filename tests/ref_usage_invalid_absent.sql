{{ config(tags = ['qa']) }}

-- Fail if any invalid rows are found in REF_USAGE_ATLAS

with

invalid as (
    select
        usage_hkey
    from {{ ref('REF_USAGE_ATLAS__invalid') }}
)

, leaked as (
    select
        ref.usage_hkey
    from {{ ref('REF_USAGE_ATLAS') }} ref
    inner join invalid using (usage_hkey)
)

select *
from leaked
