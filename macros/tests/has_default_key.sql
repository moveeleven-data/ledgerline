/**
 * has_default_key.sql
 * -------------------
 * Purpose:
 * Ensures every dimension includes the expected default key record.
 */

{% test has_default_key (
    model
  , column_name
  , default_key_value = '-1'
  , record_source_field_name = 'RECORD_SOURCE'
  , default_key_record_source = 'System.DefaultKey'
) -%}

select
      '{{ default_key_value }}'          as {{ column_name }}
    , '{{ default_key_record_source }}' as {{ record_source_field_name }}
from {{ model }}
where {{ column_name }} = '{{ default_key_value }}'
  and {{ record_source_field_name }} = '{{ default_key_record_source }}'

having count(*) = 0

{%- endtest %}