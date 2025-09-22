{% macro get_latest_usage_report_date() %}
  {# Allow override via --vars "as_of_date: 'YYYY-MM-DD'" #}
  {% if var('as_of_date', none) is not none %}
    {{ return(var('as_of_date')) }}
  {% endif %}

  {% if execute %}
    {# Use the seeded relation to stay environment-agnostic #}
    {% set sql %}
      select to_char(max(report_date), 'YYYY-MM-DD') as max_report_date
      from {{ ref('usage_daily') }}
    {% endset %}
    {% set res = run_query(sql) %}
    {% set latest_date = res.columns[0].values()[0] %}
    {{ return(latest_date) }}

  {% else %}
    {{ return('1900-01-01') }}  {# Fallback #}
  {% endif %}
{% endmacro %}
