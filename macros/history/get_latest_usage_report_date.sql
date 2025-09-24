/**
 * get_latest_usage_report_date()
 * -------------------------------
 * Returns the latest report_date (YYYY-MM-DD) from atlas_meter_usage_daily.
 * - Honors manual override via: dbt run --vars "as_of_date: 'YYYY-MM-DD'"
 * - When not executing (e.g., docs generation), returns a safe fallback.
 * - Null-safe extraction from run_query result to avoid jinja errors.
 */
 
{% macro get_latest_usage_report_date() %}

  {# If the user passed in an override (via --vars), just use that #}
  {% set as_of_date_override = var('as_of_date', none) %}

  {% if as_of_date_override is not none %}
    {{ return(as_of_date_override) }}
  {% endif %}

  {# If dbt isn’t actually running (like during docs build), fall back #}
  {% if not execute %}
    {{ return(fallback_date_default) }}
  {% endif %}

  {# Otherwise, run a query to grab the latest report_date #}
  {% set latest_report_date_sql %}
    select
          to_char(
              max(report_date)
            , 'YYYY-MM-DD'
          ) as max_report_date
    from {{ source('atlas_meter', 'atlas_meter_usage_daily') }}
  {% endset %}

  {% set query_result = run_query(latest_report_date_sql) %}

  {# Check if the query actually returned a value. If not, set latest_report_date_str to None. #}
  {% set latest_report_date_str = (
        query_result
        and query_result.columns
        and query_result.columns[0].values()
        and query_result.columns[0].values()[0]
  ) or none %}

  {# Return the value, or the default if nothing came back #}
  {{ return(latest_report_date_str if latest_report_date_str is not none else fallback_date_default) }}

{% endmacro %}
