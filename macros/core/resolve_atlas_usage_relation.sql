/**
 * resolve_atlas_usage_relation.sql
 * --------------------------------
 * Pick the upstream table for the Atlas usage feed, based on environment.
 *
 * Behavior:
 *   - Dev / QA (default): Read from the SEED model.
 *   - Prod:               Read from the real SOURCE table.
 *   - Override: set `DBT_ATLAS_USAGE_MODE=source`.
 *
 * Inputs:
 *   - target.name: The name of the current dbt environment
 *     (for example: "dev", "qa", or "prod").
 *
 *   - DBT_ATLAS_USAGE_MODE: An environment variable that lets you
 *     force the behavior no matter what the environment is.
 *
 *     Options are:
 *       - "seed"   - Use the seed model (this is the default).
 *       - "source" - Use the real source table.
 */

{% macro resolve_atlas_usage_relation() -%}

    {% set mode = env_var('DBT_ATLAS_USAGE_MODE', 'seed') | lower %}

    {% if target.name | lower == 'prod' or mode == 'source' %}
        {{ return(source('atlas_meter','atlas_meter_usage_daily')) }}
    {% else %}
        {{ return(ref('atlas_meter_usage_daily')) }}
    {% endif %}

{%- endmacro %}