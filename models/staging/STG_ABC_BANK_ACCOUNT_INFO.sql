{{ config(materialized='ephemeral') }}

with

-- base rows from the source fact feed
base as (
    select
        upper(ACCOUNTID)  as ACCOUNT_CODE,
        upper(CURRENCY)   as ACCOUNT_CURRENCY_CODE,
        REPORT_DATE
        
    from {{ source('abc_bank', 'ABC_BANK_POSITION') }}
),

-- keep the latest currency per account by report date
latest as (
    select
        ACCOUNT_CODE,
        ACCOUNT_CURRENCY_CODE

    from (
        select
            ACCOUNT_CODE,
            ACCOUNT_CURRENCY_CODE,
            REPORT_DATE,
            row_number() over (
                partition by ACCOUNT_CODE
                order by REPORT_DATE desc, ACCOUNT_CURRENCY_CODE
            ) as rn
        from base
    )
    QUALIFY rn = 1
),

-- add a default record so facts can always join
with_default as (
    select
        ACCOUNT_CODE,
        ACCOUNT_CURRENCY_CODE,

        'SOURCE_DATA.ABC_BANK_POSITION'     as RECORD_SOURCE,
        '{{ run_started_at }}'::TIMESTAMP   as LOAD_TS
    from latest

    union all

    select
        '-1'                           as ACCOUNT_CODE,
        '-1'                           as ACCOUNT_CURRENCY_CODE,
        'MISSING'                      as RECORD_SOURCE,
        TO_TIMESTAMP_NTZ('2020-01-01') as LOAD_TS
),

hashed as (
  select
    {{ dbt_utils.surrogate_key(['account_code']) }} as account_hkey,
    {{ dbt_utils.surrogate_key(['account_code', 'account_currency_code']) }} as account_hdiff,

    * exclude load_ts,
    load_ts as load_ts_utc
  from with_default
)

select * from hashed