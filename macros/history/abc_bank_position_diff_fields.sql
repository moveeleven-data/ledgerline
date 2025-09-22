{% macro ledgerline_usage_diff_fields(
      prefix                      = ''
    , report_date_expr            = "to_varchar(report_date, 'YYYY-MM-DD')"
    , units_used_expression       = none
    , included_units_expression   = none
) %}

  {#
    Surrogate keys are built from this list of fields.
    When we close a row that is no longer present in the source,
    we want those closed rows to have a new and deterministic hash.

    This macro uses a different hashing strategy for open/closed rows:
    For OPEN rows the macro returns column names like curr.units_used.
    For CLOSE_SYNTHETIC rows you can pass overrides (e.g. units_used_expression='0').
    The key is generated with constants instead of real columns.

    This guarantees hash collisions are avoided in various edge cases.
  #}

  {#
    Decide which expressions to use for each field.
    Use the override if one is provided,
    otherwise fall back to the prefixed column.
  #}

  {# Decide field expressions for OPEN vs CLOSE_SYNTHETIC #}
  {%- set units_used_field = units_used_expression
       if units_used_expression is not none
       else prefix ~ 'units_used'
  -%}

  {%- set included_units_field = included_units_expression
       if included_units_expression is not none
       else prefix ~ 'included_units'
  -%}

  {# Return the ordered list used in hash generation #}
  {% set fields = [
        prefix ~ 'customer_code'
      , prefix ~ 'product_code'
      , prefix ~ 'plan_code'
      , report_date_expr
      , units_used_field
      , included_units_field
  ] %}
  {{ return(fields) }}

{% endmacro %}