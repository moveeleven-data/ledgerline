{% macro dev_insert_position(
    accountid
  , symbol
  , exchange
  , quantity
  , cost_base
  , position_value
  , currency
  , report_date=None
) %}

  {# Pick a date. Use provided value, otherwise use run start. #}
  {% set report_date_str = report_date if report_date else run_started_at.strftime('%Y-%m-%d') %}

  {%- set sql -%}
    insert into {{ source('abc_bank','abc_bank_position') }}
    (
        accountid
      , symbol
      , exchange
      , report_date
      , quantity
      , cost_base
      , position_value
      , currency
      , ingested_at
    )
    values
    (
        '{{ accountid }}'
      , '{{ symbol }}'
      , '{{ exchange }}'
      , to_date('{{ report_date_str }}')
      , {{ quantity }}
      , {{ cost_base }}
      , {{ position_value }}
      , '{{ currency }}'
      , current_timestamp()
    )
  {%- endset -%}

  {{ log('Executing: ' ~ sql, info=True) }}
  {{ run_query(sql) }}

{% endmacro %}

{% macro dev_delete_position(
    symbol
  , report_date=None
) %}

  {% set report_date_str = report_date if report_date else run_started_at.strftime('%Y-%m-%d') %}

  {%- set sql -%}
    delete from {{ source('abc_bank','abc_bank_position') }}
    where symbol = '{{ symbol }}'
      and report_date = to_date('{{ report_date_str }}')
  {%- endset -%}

  {{ log('Executing: ' ~ sql, info=True) }}
  {{ run_query(sql) }}

{% endmacro %}
