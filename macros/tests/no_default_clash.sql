/**
 * no_default_clash.sql
 * --------------------
 * Ensures that the default surrogate key (`-1`) is only ever associated
 * with the designated default record source (`System.DefaultKey`).
 *
 * Usage:
 * Applied as a generic test on refined dimension models to guarantee
 * clean handling of default members.
 *
 * Notes:
 * - Each real dimension row carries a record_source identifying its lineage
 *   (for example, 'SEED.atlas_catalog_product_info' in dim_product).
 * - The synthetic default row must always use 'System.DefaultKey' instead.
 */

{% test no_default_clash(
      model
    , surrogate_key_column
    , lineage_source_column='RECORD_SOURCE'
    , default_surrogate_key='-1'
    , default_lineage_source='System.DefaultKey'
) %}

select *
from {{ model }}
where {{ surrogate_key_column }} = '{{ default_surrogate_key }}'
  and {{ lineage_source_column }} != '{{ default_lineage_source }}'

{% endtest %}
