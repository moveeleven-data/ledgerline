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

    {# Step 1. Respect a manual override.
       If someone runs dbt with --vars "as_of_date: 'YYYY-MM-DD'",
       we stop here and return that value. This allows reproducible backfills
       or debugging against a specific date, instead of always using "latest". #}

    {% set as_of_date_override = var('as_of_date', none) %}

    {% if as_of_date_override is not none %}
        {{ return(as_of_date_override) }}
    {% endif %}


    -- 2. If not running SQL (like in docs build), return predefined fallback date.

    {% if not execute %}
        {{ return(fallback_date_default) }}
    {% endif %}


    -- 3. Build query to find the max report_date from the usage feed.

    {% set latest_report_date_sql %}
        select
              to_char(
                    max(report_date)
                  , 'YYYY-MM-DD'
              ) as max_report_date
        from {{ source('atlas_meter', 'atlas_meter_usage_daily') }}
    {% endset %}

    {% set query_result = run_query(latest_report_date_sql) %}


    -- 4. Pull the first value from the result, if any.

    {% set latest_report_date_str = (
          query_result
          and query_result.columns
          and query_result.columns[0].values()
          and query_result.columns[0].values()[0]
    ) or none %}


    -- 5. Return the date we found, or fallback if it was null.
    
    {{ return(latest_report_date_str if latest_report_date_str is not none else fallback_date_default) }}

{% endmacro %}