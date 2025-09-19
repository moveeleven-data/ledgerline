{{ config(
    materialized           = 'incremental'
  , incremental_strategy   = 'merge'
  , unique_key             = ['POSITION_HKEY','REPORT_DATE','POSITION_ROW_TYPE']
  , on_schema_change       = 'ignore'
  , merge_update_columns   = [
        'POSITION_HDIFF'
      , 'ACCOUNT_CODE'     , 'SECURITY_CODE' , 'SECURITY_NAME'
      , 'EXCHANGE_CODE'    , 'CURRENCY_CODE' , 'RECORD_SOURCE'
      , 'QUANTITY'         , 'COST_BASE'     , 'POSITION_VALUE'
      , 'POSITION_ROW_TYPE'
    ]
  , tags                   = ['history','positions']
) }}

{# Resolve the trading day to process #}

{% set as_of_date              = get_latest_position_report_date() %}
{% set as_of_date_literal      = "to_date('" ~ as_of_date ~ "')" %}
{% set as_of_date_varchar_expr = "to_varchar(" ~ as_of_date_literal ~ ", 'YYYY-MM-DD')" %}

{# Fields used to hash a synthetic CLOSE row (zeros + fixed date) #}

{% set position_diff_fields_close = abc_bank_position_diff_fields(
      prefix='prior.'
    , report_date_expr          = as_of_date_varchar_expr
    , quantity_expression       = '0'
    , cost_base_expression      = '0'
    , position_value_expression = '0'
) %}

with

  -- Today's OPENs (already de-duped in STG)
  -- Only consider rows for the trading day we are processing 
  stg_today as (
    select
        stg.position_hkey
      , stg.position_hdiff
      , stg.account_code
      , stg.security_code
      , stg.security_name
      , stg.exchange_code
      , stg.currency_code
      , stg.record_source
      , stg.report_date
      , stg.quantity
      , stg.cost_base
      , stg.position_value
      , stg.load_ts_utc
      , 'OPEN'::string as position_row_type
    from {{ ref('STG_ABC_BANK_POSITION') }} as stg
    where stg.report_date = {{ as_of_date_literal }}
  )

{% if is_incremental() %}

-- Find the latest OPEN strictly before as_of_date, by business key
, prior_latest_open as (
  {{ latest_prior_open_sql(this, as_of_date_literal) }}
)

, today_keys as (
  select distinct position_hkey
  from stg_today
)

-- Detect positions missing from current batch and create synthetic CLOSE records
, closed_positions as (
   {{ synthetic_close_select_sql(
      'prior_latest_open'
     ,'today_keys'
     , as_of_date_literal
     , position_diff_fields_close
   ) }}
)

, changes_to_store as (
  select *
  from stg_today
  union all
  select *
  from closed_positions
)
 
{% else %} -- full-refresh or first build: just take the day’s snapshot as OPEN

, changes_to_store as (
  select *
  from stg_today
)

{% endif %}

-- Deduplicate by keeping the latest load_ts_utc per key/date/type combo
, changes_dedup as (
  select *
  from changes_to_store
  qualify row_number() over (
            partition by
                 position_hkey
               , report_date
               , position_row_type
            order by
                 load_ts_utc desc
               , position_hdiff desc
          ) = 1
)

select *
from changes_dedup
