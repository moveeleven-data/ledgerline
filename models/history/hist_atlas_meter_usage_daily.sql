{{ config(
     materialized = 'incremental'
   , incremental_strategy = 'append'
   , on_schema_change = 'ignore'
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

{% set as_of_date         = get_latest_usage_report_date() %}
{% set as_of_date_literal = "to_date('" ~ as_of_date ~ "')" %}

with stg_today as (
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
        , ingestion_ts
    from {{ ref('stg_atlas_meter_usage_daily') }}
    where report_date = {{ as_of_date_literal }}
)

select * from stg_today
