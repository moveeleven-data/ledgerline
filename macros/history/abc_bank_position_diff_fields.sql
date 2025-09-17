{% macro abc_bank_position_diff_fields(
    prefix=''
  , report_date_expr="to_varchar(report_date, 'YYYY-MM-DD')"
  , quantity_expression=None
  , cost_base_expression=None
  , position_value_expression=None
) %}

  {# 
    Surrogate keys are built from this list of fields.
    When we close a row that is no longer present in the source,
    we want those closed rows to have a new and deterministic hash.

    This macro uses a different hashing strategy for open/closed rows:
    For OPEN rows the macro returns column names like curr.quantity.
    For CLOSE_SYNTHETIC rows you can pass overrides (e.g. quantity_expression='0')
    The key is generated with constants instead of real columns.

    This guarantees hash collisions are avoided in various edge cases.
  #}


  {# 
    Decide which expressions to use for each field.
    Use the override if one is provided,
    otherwise fall back to the prefixed column.
  #}

  {%- set quantity_field = quantity_expression
       if quantity_expression is not none
     else prefix ~ 'quantity'
  -%}

  {%- set cost_base_field = cost_base_expression
       if cost_base_expression is not none
     else prefix ~ 'cost_base'
  -%}

  {%- set position_value_field = position_value_expression
       if position_value_expression is not none
     else prefix ~ 'position_value'
  -%}

  {# build the list of SQL expressions (as strings), then return it #}

  {% set fields = [
        prefix ~ 'account_code'
      , prefix ~ 'security_code'
      , prefix ~ 'security_name'
      , prefix ~ 'exchange_code'
      , report_date_expr
      , quantity_field
      , cost_base_field
      , position_value_field
      , prefix ~ 'currency_code'
  ] %}

  {{ return(fields) }}

{% endmacro %}
