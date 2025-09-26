/**
 * dev_delete_usage.sql
 * --------------------
 * Helper macro for local development.
 * Deletes synthetic usage rows from usage_daily.
 *
 * - If report_date is not provided, defaults to the run start date.
 * - Matches rows by customer, product, plan, and report_date.
 */

{% macro dev_delete_usage(
      customer_code
    , product_code
    , plan_code
    , report_date = none
) %}

  {% set report_date_str = report_date if report_date else run_started_at.strftime('%Y-%m-%d') %}

  {%- set sql -%}

    delete from {{ ref('usage_daily') }}

    where 1=1
      and customer_code = '{{ customer_code | upper }}'
      and product_code  = '{{ product_code  | upper }}'
      and plan_code     = '{{ plan_code     | upper }}'
      and report_date   = to_date('{{ report_date_str }}')

  {%- endset -%}


  {{ log('Executing: ' ~ sql, info = true) }}

  {{ run_query(sql) }}

{% endmacro %}
