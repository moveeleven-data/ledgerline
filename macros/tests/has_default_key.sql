{% test has_default_key (
    model
  , column_name
  , default_key_value = '-1'
  , record_source_field_name = 'RECORD_SOURCE'
  , default_key_record_source = 'System.DefaultKey'
) -%}

with

default_key_rows as (
    select distinct {{ column_name }}
                  , {{ record_source_field_name }}

    from {{ model }}
    where {{ column_name }}              = '{{ default_key_value }}'
      and {{ record_source_field_name }} = '{{ default_key_record_source }}'
),

validation_errors as (
    select '{{ default_key_value }}'        as {{column_name }}
         , '{{default_key_record_source }}' as {{ record_source_field_name }}

    except

    select {{ column_name }}
         , {{ record_source_field_name }}
    from default_key_rows
)

select *
from validation_errors

{%- endtest %}