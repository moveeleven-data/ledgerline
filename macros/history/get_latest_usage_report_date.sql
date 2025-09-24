/**
 * get_latest_usage_report_date.sql
 * --------------------------------
 * Returns the latest report_date (YYYY-MM-DD) from atlas_meter_usage_daily.
 *
 * Behavior:
 * - Honors manual override via: dbt run --vars "as_of_date: 'YYYY-MM-DD'"
 * - When not executing (e.g. during docs generation), returns a safe fallback.
 * - Queries atlas_meter_usage_daily for the max(report_date) when executing.
 * - Null-safe: falls back if query result is empty.
 */

{% macro get_latest_usage_report_date() %}

    {# 1. Check for manual override. If the user passed in an override (via --vars), just use that. #}
    {% set as_of_date_override = var('as_of_date', none) %}
    {% if as_of_date_override is not none %}
        {{ return(as_of_date_override) }}
    {% endif %}

    -- 2. If not running SQL (e.g. docs build), return fallback.
    {% if not execute %}
        {{ return(fallback_date_default) }}
    {% endif %}

    -- 3. Query the latest report_date from source.
    {% set latest_report_date_sql %}
        select
              to_char(max(report_date), 'YYYY-MM-DD') as max_report_date
        from {{ source('atlas_meter', 'atlas_meter_usage_daily') }}
    {% endset %}

    {% set query_result = run_query(latest_report_date_sql) %}

    -- 4. Extract result.
    {% set latest_report_date_str = (
          query_result
          and query_result.columns
          and query_result.columns[0].values()
          and query_result.columns[0].values()[0]
    ) or none %}

    -- 5. Return result.
    {{ return(latest_report_date_str if latest_report_date_str is not none else fallback_date_default) }}

{% endmacro %}