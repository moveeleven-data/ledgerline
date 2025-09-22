{% macro get_latest_usage_report_date() %}
  {# 
    Returns the latest report_date from atlas_meter_usage_daily.
    - Allows override via --vars "as_of_date: 'YYYY-MM-DD'"
    - Defaults to '1900-01-01' if no rows are present or if not executing
  #}

  {# Allow explicit override #}
  {% if var('as_of_date', none) is not none %}
    {{ return(var('as_of_date')) }}
  {% endif %}

  {# Query max(report_date) if executing in dbt run context #}
  {% if execute %}
    {% set sql %}
      select
          to_char(max(report_date), 'YYYY-MM-DD') as max_report_date
      from {{ source('atlas_meter', 'atlas_meter_usage_daily') }}
    {% endset %}

    {% set res = run_query(sql) %}

    {# Handle empty results #}
    {% set has_val = res and res.columns and res.columns[0].values()|length > 0 %}
    {% set val = has_val and res.columns[0].values()[0] or none %}

    {{ return(val if val is not none else '1900-01-01') }}
  {% else %}

    {{ return('1900-01-01') }}  {# Fallback #}
  {% endif %}

{% endmacro %}
