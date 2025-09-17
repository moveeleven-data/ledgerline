{% macro abc_bank_position_diff_fields() %}
  [
    'account_code'
    , 'security_code'
    , 'security_name'
    , 'exchange_code'
    , "to_varchar(report_date, 'YYYY-MM-DD')"
    , 'quantity'
    , 'cost_base'
    , 'position_value'
    , 'currency_code'
  ]
{% endmacro %}