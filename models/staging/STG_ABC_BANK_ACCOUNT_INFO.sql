{{ config(materialized='ephemeral') }}

with src as (
  select
    upper(account_code)               as account_code,
    upper(account_currency_code)      as account_currency_code,
    '{{ run_started_at }}'::timestamp as load_ts
  from {{ ref('abc_bank_account_info') }}
),

with_default as (
  select
    account_code,
    account_currency_code,
    'SEED.abc_bank_account_info' as record_source,
    load_ts
  from src
  union all
  select
    '-1', '-1', 'System.DefaultKey', to_timestamp_ntz('2020-01-01')
),

hashed as (
  select
    {{ dbt_utils.generate_surrogate_key(['account_code']) }} as account_hkey,
    {{ dbt_utils.generate_surrogate_key(['account_code','account_currency_code']) }} as account_hdiff,
    * exclude (load_ts),
    load_ts as load_ts_utc
  from with_default
)

select * from hashed
