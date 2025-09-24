/**
 * warn_on_multiple_default_key.sql
 * --------------------------------
 * Warns if non-default surrogate keys are tagged with the reserved
 * default record source (`System.DefaultKey`).
 *
 * Usage:
 * Applied as a generic test on refined dimension models to ensure
 * that only the synthetic default row uses the reserved
 * 'System.DefaultKey' record source.
 */

{% test warn_on_multiple_default_key (
      model
    , surrogate_key_column
    , default_surrogate_key_value = '-1'
    , lineage_source_column = 'RECORD_SOURCE'
    , default_lineage_source_value = 'System.DefaultKey'
) -%}

{{ config(severity = 'warn') }}

with invalid_default_source_usage as (
    select distinct
          {{ surrogate_key_column }}
        , {{ lineage_source_column }}
    from {{ model }}
    where {{ surrogate_key_column }} != '{{ default_surrogate_key_value }}'
      and {{ lineage_source_column }} = '{{ default_lineage_source_value }}'
)

select *
from invalid_default_source_usage

{%- endtest %}
