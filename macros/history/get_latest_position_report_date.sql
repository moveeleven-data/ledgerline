{% macro get_latest_position_report_date() %}
  {# If the user passed --vars "as_of_date: 'YYYY-MM-DD'", use that #}
  {% if var('as_of_date', none) is not none %}
    {{ return(var('as_of_date')) }}
  {% endif %}

  {# Otherwise, use the latest report_date from the raw source #}
  {% if execute %}
    {% set sql %}
      select to_char(max(report_date), 'YYYY-MM-DD') as max_report_date
      from MARKET_SYNC_DEV.SOURCE_DATA.ABC_BANK_POSITION
    {% endset %}
    {% set res = run_query(sql) %}
    {% set latest_date = res.columns[0].values()[0] %}
    {{ return(latest_date) }}

  {% else %}
    {{ return('1900-01-01') }}  {# fallback #}
  {% endif %}

{% endmacro %}
