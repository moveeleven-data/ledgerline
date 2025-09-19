{{ config(
    materialized = 'incremental'
  , incremental_strategy = 'merge'
  , unique_key = ['POSITION_HKEY','REPORT_DATE','POSITION_ROW_TYPE']
) }}

{# Resolve the trading day to process #}
{% set as_of_date = get_latest_position_report_date() %}
{% set as_of_date_literal = "to_date('" ~ as_of_date ~ "')" %}
{% set as_of_date_varchar_expr = "to_varchar(" ~ as_of_date_literal ~ ", 'YYYY-MM-DD')" %}

{# Fields used to hash a synthetic CLOSE row (zeros + fixed date) #}
{% set position_diff_fields_close = abc_bank_position_diff_fields(
      prefix='prior.'
    , report_date_expr=as_of_date_varchar_expr
    , quantity_expression='0'
    , cost_base_expression='0'
    , position_value_expression='0'
) %}


with

-- Base staging input with explicit column overrides
stg_input as (
    select 
          stg.* exclude (report_date, quantity, cost_base, position_value, load_ts_utc)
        , report_date
        , quantity
        , cost_base
        , position_value
        , load_ts_utc
        , 'OPEN' AS position_row_type
    from {{ ref('STG_ABC_BANK_POSITION') }} as stg
)

-- Only consider rows for the trading day we’re processing
, stg_today as (
  select *
  from stg_input
  where report_date = {{ as_of_date_literal }} 
)


{% if is_incremental() %}

-- Find the latest OPEN strictly before as_of_date, by business key
, prior_latest_open as (
  select *
  from {{ this }}
  where position_row_type = 'OPEN'
    and report_date       < {{ as_of_date_literal }}
  qualify row_number() over (
           partition by position_hkey
           order by
                report_date desc
              , load_ts_utc desc
         ) = 1
)

, today_keys as (
  select distinct position_hkey
  from stg_today
)

-- Detect new rows (not already in history by diff hash)
, new_rows as (
   select stg.*
   from stg_today as stg
)

-- Detect positions missing from current batch and create synthetic CLOSE records
, closed_positions as (
  select
      prior.position_hkey                                                 as position_hkey
    , {{ dbt_utils.generate_surrogate_key(position_diff_fields_close) }}  as position_hdiff
    , prior.account_code                                                  as account_code
    , prior.security_code                                                 as security_code
    , prior.security_name                                                 as security_name
    , prior.exchange_code                                                 as exchange_code
    , prior.currency_code                                                 as currency_code
    , prior.record_source                                                 as record_source
    , {{ as_of_date_literal }}                                            as report_date
    , 0                                                                   as quantity
    , 0                                                                   as cost_base
    , 0                                                                   as position_value
    , '{{ run_started_at }}'::timestamp_ntz                               as load_ts_utc
    , 'CLOSE_SYNTHETIC'                                                   as position_row_type
  from prior_latest_open prior
  left join today_keys tk
    on tk.position_hkey = prior.position_hkey
  where tk.position_hkey is null
)

, new_rows_projected as (
  select
      position_hkey
    , position_hdiff
    , account_code
    , security_code
    , security_name
    , exchange_code
    , currency_code
    , record_source
    , report_date
    , quantity
    , cost_base
    , position_value
    , load_ts_utc
    , position_row_type
  from new_rows
)

, changes_to_store as (
  select * from new_rows_projected
  union all
  select * from closed_positions
)

{% else %}  -- full-refresh or first build: just take the day’s snapshot as OPEN

, changes_to_store as (
  select
      position_hkey
    , position_hdiff
    , account_code
    , security_code
    , security_name
    , exchange_code
    , currency_code
    , record_source
    , report_date
    , quantity
    , cost_base
    , position_value
    , load_ts_utc
    , position_row_type
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

select
    position_hkey
  , position_hdiff
  , account_code
  , security_code
  , security_name
  , exchange_code
  , currency_code
  , record_source
  , report_date
  , quantity
  , cost_base
  , position_value
  , load_ts_utc
  , position_row_type
from changes_dedup