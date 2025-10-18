{{ config(
     materialized = 'incremental'
   , incremental_strategy = 'merge'
   , unique_key = ['usage_hkey', 'report_date']
   , on_schema_change = 'sync_all_columns'
) }}

/**
 * hist_atlas_meter_usage_daily.sql
 * --------------------------------
 * History table for daily metered usage feed.
 *
 * Purpose:
 * - Stores what arrived and when, one row per key and day per load.
 * - REF models pick the latest version when needed.
 * - Append only.
 */

-- Pull fallback date from environment variables
{% set fallback_date_default = var('fallback_date_default', '2000-01-01') %}

-- Optionally accept an override date
{% set as_of_date_override   = var('as_of_date', none) %}

-- Use the override date if provided
{% if as_of_date_override is not none %}
  {% set as_of_date_literal = "to_date('" ~ as_of_date_override ~ "')" %}

-- Otherwise, default to the max available report_date in staging,
-- or fallback_date_default if staging is empty
{% else %}
  {% set as_of_date_literal -%}
    (
      select coalesce(
                 max(report_date)
               , to_date('{{ fallback_date_default }}')
             )
      from {{ ref('stg_atlas_meter_usage_daily') }}
    )
  {%- endset %}
{% endif %}

with

stg_today as (
    select
          usage_hkey
        , usage_hdiff
        , customer_hkey
        , product_hkey
        , plan_hkey
        , customer_code
        , product_code
        , plan_code
        , record_source
        , report_date
        , units_used
        , included_units
        , load_ts_utc
        , 'OPEN'::string as usage_row_type
    from {{ ref('stg_atlas_meter_usage_daily') }}
    
    where
        report_date = {{ as_of_date_literal }}
)

select
    *
from stg_today

