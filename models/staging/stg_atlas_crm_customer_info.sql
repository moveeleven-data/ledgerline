-- stg_atlas_crm_customer_info (corrected source)
{{ config(materialized='ephemeral') }}

with src as (
  select
      upper(customer_code)             as customer_code
    , customer_name
    , upper(country_code)              as country_code
    , to_timestamp_ntz(load_ts)        as load_ts
    , 'SEED.atlas_crm_customer_info'   as record_source
  from {{ ref('customers') }}
),

default_row as (
  select '-1'
        ,'Missing'
        ,'-1'
        ,to_timestamp_ntz('2020-01-01')
        ,'System.DefaultKey'
),

unioned as (
  select * from src
  union all
  select * from default_row
),

hashed as (
  select
      {{ dbt_utils.generate_surrogate_key(['customer_code']) }} as customer_hkey
    , {{ dbt_utils.generate_surrogate_key([
           'customer_code'
          ,'customer_name'
          ,'country_code'
      ]) }} as customer_hdiff

    , * exclude (load_ts)
    , load_ts as load_ts_utc
  from unioned
)

select * from hashed
