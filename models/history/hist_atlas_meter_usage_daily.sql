{{ declare_usage_lineage_dependencies() }}

{{ config(
    incremental_strategy = 'merge',
    unique_key = ['usage_hkey', 'report_date', 'usage_row_type'],
    merge_update_columns = [
         'usage_hdiff'
       , 'customer_code'
       , 'product_code'
       , 'plan_code'
       , 'record_source'
       , 'units_used'
       , 'included_units'
       , 'usage_row_type'
    ]
) }}


/**
 * hist_atlas_meter_usage_daily.sql
 * --------------------------------
 * History table for daily metered usage feed.
 *
 * Purpose:
 * - Capture daily OPEN usage rows (deduped at staging).
 * - Merge incrementally by surrogate key + date + row type.
 * - On each load:
 *   * Insert today's OPEN rows.
 *   * Generate synthetic CLOSE rows for keys missing today.
 * - Deduplicate by load_ts_utc to keep the latest.
 *
 * Keys:
 * - usage_hkey = stable surrogate ID.
 * - usage_hdiff = detects changes to usage values.
 */

{# Resolve the usage day to process #}

{% set as_of_date              = get_latest_usage_report_date() %}
{% set as_of_date_literal      = "to_date('" ~ as_of_date ~ "')" %}
{% set as_of_date_varchar_expr = "to_varchar(" ~ as_of_date_literal ~ ", 'YYYY-MM-DD')" %}

{# Fields used to hash a synthetic CLOSE row (zeros + fixed date) #}

{% set usage_diff_fields_close  = ledgerline_usage_diff_fields(
      prefix                    = 'prior.'
    , report_date_expr          = as_of_date_varchar_expr
    , units_used_override       = '0'
    , included_units_override   = '0'
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
    where
          stg.report_date =  {{ as_of_date_literal }}
      and stg.units_used     is not null
      and stg.included_units is not null
)

{% if is_incremental() %}

  -- Latest OPEN strictly before as_of_date by usage business key

, prior_latest_open as (
    {{ latest_prior_open(
          history_relation = this
        , as_of_date_literal = as_of_date_literal
    ) }}
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
    {{ synthetic_close(
          rows_from_yesterday    = 'prior_latest_open'
        , keys_from_today        = 'today_keys'
        , todays_date            = as_of_date_literal
        , fields_for_close_hash  = usage_diff_fields_close
    ) }}
)

, changes_to_store as (
    select
      *
    from stg_today

    union all

    select
      *
    from closed_usages
)

{% else %}  -- first load, take the day’s snapshot as OPEN

, changes_to_store as (
    select
      *
    from stg_today
)

{% endif %}

  -- Deduplicate, keep latest load_ts_utc per key, date, and row type

, changes_dedup as (
    select
        *
    from changes_to_store

    qualify
        row_number() over (
            partition by
            usage_hkey
          , report_date
          , usage_row_type
        order by
            load_ts_utc desc
          , usage_hdiff desc
    ) = 1
)

select
    usage_hkey
  , usage_hdiff
  , customer_code
  , product_code
  , plan_code
  , record_source
  , report_date
  , units_used
  , included_units
  , load_ts_utc
  , usage_row_type
from changes_dedup