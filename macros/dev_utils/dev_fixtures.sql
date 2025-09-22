{% macro dev_insert_usage(
      customer_code
    , product_code
    , plan_code
    , units_used
    , included_units
    , report_date = none
) %}

  {# Pick a date. Use provided value, otherwise use run start. #}
  {% set report_date_str = report_date if report_date else run_started_at.strftime('%Y-%m-%d') %}

  {%- set sql -%}
    insert into {{ ref('usage_daily') }}
    (
          customer_code
        , product_code
        , plan_code
        , report_date
        , units_used
        , included_units
        , load_ts
    )
    values
    (
          '{{ customer_code | upper }}'
        , '{{ product_code  | upper }}'
        , '{{ plan_code     | upper }}'
        , to_date('{{ report_date_str }}')
        , {{ units_used }}
        , {{ included_units }}
        , current_timestamp()
    )
  {%- endset -%}

  {{ log('Executing: ' ~ sql, info = true) }}
  {{ run_query(sql) }}
{% endmacro %}

{% macro dev_delete_usage(
      customer_code
    , product_code
    , plan_code
    , report_date = none
) %}

  {% set report_date_str = report_date if report_date else run_started_at.strftime('%Y-%m-%d') %}

  {%- set sql -%}
    delete from {{ ref('usage_daily') }}
    where customer_code = '{{ customer_code | upper }}'
      and product_code  = '{{ product_code  | upper }}'
      and plan_code     = '{{ plan_code     | upper }}'
      and report_date   = to_date('{{ report_date_str }}')
  {%- endset -%}

  {{ log('Executing: ' ~ sql, info = true) }}
  {{ run_query(sql) }}

{% endmacro %}

