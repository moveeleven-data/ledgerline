/**
 * dev_delete_usage.sql
 * --------------------
 * Helper macro for local development.
 * Deletes synthetic usage rows from usage_daily.
 * Includes basic guardrails to prevent execution outside dev targets.
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

    {% if target.name not in ['dev','qa'] %}
        {{ exceptions.raise_compiler_error('dev_delete_usage is blocked outside dev/qa targets') }}
    {% endif %}

    {% if not var('confirm', false) %}
        {{ exceptions.raise_compiler_error("Set var('confirm', true) to run dev_delete_usage") }}
    {% endif %}

    {% set usage_rel = ref('usage_daily') %}
    {% if usage_rel.schema != target.schema %}
        {{ exceptions.raise_compiler_error(
              'dev_delete_usage must target the current schema: '
            ~ target.schema
            ~ ' (got ' ~ usage_rel.schema ~ ')'
    ) }}
    {% endif %}

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