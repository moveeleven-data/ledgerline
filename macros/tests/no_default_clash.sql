{% test no_default_clash(
      model
    , code_col
    , record_source_col='RECORD_SOURCE'
    , default_code='-1'
    , default_source='System.DefaultKey'
) %}

select *
from {{ model }}
where {{ code_col }} = '{{ default_code }}'
  and {{ record_source_col }} != '{{ default_source }}'

{% endtest %}