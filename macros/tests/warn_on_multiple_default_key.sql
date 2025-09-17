{% test warn_on_multiple_default_key (
      model
    , column_name
    , default_key_value = '-1'
    , record_source_field_name = 'RECORD_SOURCE'
    , default_key_record_source = 'System.DefaultKey'
) -%}

{{ config(severity = 'warn') }}

with

validation_errors as (
    select distinct {{ column_name }}
                  , {{ record_source_field_name }}

    from {{ model }}
    where {{ column_name }}             != '{{ default_key_value }}'
      and {{ record_source_field_name }} = '{{default_key_record_source}}'
)

select *
from validation_errors

{%- endtest %}