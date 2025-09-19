{{ config(tags=['qa']) }}

-- Fail if any invalid rows are found in REF_POSITION_ABC_BANK

with

invalid as (
    select position_hkey
    from {{ ref('REF_POSITION_ABC_BANK__invalid') }}
)

, leaked as (
    select ref.position_hkey
    from {{ ref('REF_POSITION_ABC_BANK') }} ref
    inner join invalid using (position_hkey)
)

select * from leaked