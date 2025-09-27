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

    {% set fallback_date_default = var('fallback_date_default', '2000-01-01') %}

    /* Step 1. Respect a manual override.
       If someone runs dbt with --vars "as_of_date: 'YYYY-MM-DD'"
       we stop here and return that value. This allows reproducible backfills. */

    {% set as_of_date_override = var('as_of_date', none) %}
    {% if as_of_date_override is not none %}
        {{ return(as_of_date_override) }}
    {% endif %}

    -- 2. If compiling (docs build or parse-only), return predefined fallback date.

    {% if not execute %}
        {{ return(fallback_date_default) }}
    {% endif %}

    -- 3. Build query to find the max report_date, with a SQL-level fallback.

    {% set latest_report_date_sql %}
        select to_char(
                 coalesce(max(report_date), to_date('{{ fallback_date_default }}')),
                 'YYYY-MM-DD'
               ) as max_report_date
        from {{ ref('stg_atlas_meter_usage_daily') }}
    {% endset %}

    -- 4. Execute query and capture result.

    {% set query_output = run_query(latest_report_date_sql) %}

    -- 5. Return the latest report_date if found.

    {% set latest_date = none %}

    {% if query_output and query_output.columns and query_output.columns[0].values() %}
        {% set latest_date = query_output.columns[0].values() | first %}
    {% endif %}

    {{ return(latest_date or fallback_date_default) }}

{% endmacro %}