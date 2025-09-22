{{ config(
      materialized           = 'incremental'
    , incremental_strategy   = 'merge'
    , unique_key             = ['usage_hkey','report_date','usage_row_type']
    , on_schema_change       = 'ignore'
    , merge_update_columns   = [
          'usage_hdiff'
        , 'customer_code'
        , 'product_code'
        , 'plan_code'
        , 'record_source'
        , 'units_used'
        , 'included_units'
        , 'usage_row_type'
      ]
    , tags = ['history','usage']
) }}

{# Resolve the usage day to process #}
{% set as_of_date              = get_latest_usage_report_date() %}
{% set as_of_date_literal      = "to_date('" ~ as_of_date ~ "')" %}
{% set as_of_date_varchar_expr = "to_varchar(" ~ as_of_date_literal ~ ", 'YYYY-MM-DD')" %}

{# Fields used to hash a synthetic CLOSE row (zeros + fixed date) #}
{% set usage_diff_fields_close  = ledgerline_usage_diff_fields(
      prefix                    = 'prior.'
    , report_date_expr          = as_of_date_varchar_expr
    , units_used_expression     = '0'
    , included_units_expression = '0'
) %}

with

  -- Today's OPEN usage rows, already de-duped in STG
  -- Only rows for the processing date
stg_today as (
  select
      stg.usage_hkey
    , stg.usage_hdiff
    , stg.customer_code
    , stg.product_code
    , stg.plan_code
    , stg.record_source
    , stg.report_date
    , stg.units_used
    , stg.included_units
    , stg.load_ts_utc
    , 'OPEN'::string as usage_row_type
  from {{ ref('stg_atlas_meter_usage_daily') }} as stg
  where stg.report_date = {{ as_of_date_literal }}
    and stg.units_used is not null
    and stg.included_units is not null
)

{% if is_incremental() %}

  -- Latest OPEN strictly before as_of_date by usage business key
, prior_latest_open as (
  {{ latest_prior_usage_open_sql(this, as_of_date_literal) }}
)

  -- Keys present today
, today_keys as (
  select distinct
        customer_code
      , product_code
      , plan_code
  from stg_today
)

  -- Synthetic CLOSE rows for keys missing today
, closed_usages as (
  {{ synthetic_close_usage_select_sql(
        'prior_latest_open'
      , 'today_keys'
      , as_of_date_literal
      , usage_diff_fields_close
  ) }}
)

, changes_to_store as (
  select *
  from stg_today

  union all

  select *
  from closed_usages
)

{% else %}  -- first load, take the day’s snapshot as OPEN

, changes_to_store as (
  select *
  from stg_today
)

{% endif %}

  -- Deduplicate, keep latest load_ts_utc per key, date, and row type
, changes_dedup as (
  select *
  from changes_to_store
  qualify row_number() over (
            partition by
                 usage_hkey
               , report_date
               , usage_row_type
            order by
                 load_ts_utc desc
               , usage_hdiff desc
          ) = 1
)

select  *
from changes_dedup
