/**
 * dev_insert_usage.sql
 * --------------------
 * Helper macro for local development.
 * Inserts a synthetic usage row into usage_daily.
 * Includes basic guardrails to prevent execution outside dev targets.
 *
 * - If report_date is not provided, defaults to the run start date.
 * - Uppercases customer, product, and plan codes.
 * - Uses current_timestamp for load_ts.
 */


{% macro dev_insert_usage(
      customer_code
    , product_code
    , plan_code
    , units_used
    , included_units
    , report_date = none
) %}

    {% if target.name not in ['dev', 'qa'] %}
        {{ exceptions.raise_compiler_error('dev_insert_usage is blocked outside dev/qa targets') }}
    {% endif %}

    {% if not var('confirm', false) %}
        {{ exceptions.raise_compiler_error("Set var('confirm', true) to run dev_insert_usage") }}
    {% endif %}

    {% set usage_rel = ref('usage_daily') %}
    {% if usage_rel.schema != target.schema %}
        {{ exceptions.raise_compiler_error(
                'dev_insert_usage must target the current schema: '
              ~ target.schema
              ~ ' (got ' ~ usage_rel.schema ~ ')'
        ) }}
    {% endif %}

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
