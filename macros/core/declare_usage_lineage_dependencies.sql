{% macro declare_usage_lineage_dependencies() -%}
    -- depends_on: {{ ref('atlas_meter_usage_daily_seed') }}
    -- depends_on: {{ source('atlas_meter','atlas_meter_usage_daily') }}
{%- endmacro %}