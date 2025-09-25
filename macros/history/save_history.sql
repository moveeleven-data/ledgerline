/**
 * save_history.sql
 * ----------------
 * Appends new versions from staging into the history table.
 *
 * - First run: Loads all rows from the input relation.
 *
 * - Incremental run:
 *   - Finds latest version of each key in history.
 *   - Loads only rows not already present.
 *
 * - Final output: Rows to append, optionally ordered.
 *
 * Called from a dimension model, e.g.:
 *
 *     {{ save_history(
 *           input_relation       = ref('stg_atlas_country_info')
 *         , surrogate_key_column = 'country_hkey'
 *         , diff_hash_column     = 'country_hdiff'
 *     ) }}
 */

 {#
  Parameters:
    staging_relation:          Staging model providing new rows
    surrogate_key_column:      Surrogate key in history (e.g. country_hkey)
    version_hash_column:       Diff hash to detect changes (e.g. country_hdiff)
    load_timestamp_column:     Recency column (default LOAD_TS_UTC)
    staging_filter_condition:  Optional WHERE for staging
    history_filter_condition:  Optional WHERE for history
    high_watermark_column:     Column to filter staging >= max(history)
    high_watermark_operator:   Operator for high-watermark filter
    order_by_expression:       Optional ORDER BY in final output
#}

{% macro save_history(
      staging_relation
    , surrogate_key_column
    , version_hash_column
    , load_timestamp_column     = 'LOAD_TS_UTC'
    , staging_filter_condition  = 'true'
    , history_filter_condition  = 'true'
    , high_watermark_column     = none
    , high_watermark_operator   = '>='
    , order_by_expression       = none
) -%}

with

{%- if is_incremental() %}

-- 1. Select the latest versions currently stored in history

latest_history_versions as (
    {{ current_from_history(
          history_relation         = this
        , key_column               = surrogate_key_column
        , selection_expression     = version_hash_column
        , load_timestamp_column    = load_timestamp_column
        , history_filter_condition = history_filter_condition
    ) }}
)


-- 2. Apply base staging filters.

, filtered_staging as (
    select *
    from {{ staging_relation }} as staging_row
    where {{ staging_filter_condition }}
)


-- 3. Apply high watermark if configured.

, watermarked_staging as (
    select *
    from filtered_staging

    {% if high_watermark_column %}
    where {{ high_watermark_column }} {{ high_watermark_operator }} (
              select max({{ high_watermark_column }})
              from {{ this }}
          )
    {% endif %}

)


-- 4. Keep only rows not already in history.

, staging_rows_to_insert as (
    select
          staging_row.*
    from watermarked_staging as staging_row
    left join latest_history_versions as history_version
           on history_version.{{ version_hash_column }} = staging_row.{{ version_hash_column }}
    where history_version.{{ version_hash_column }} is null
)

{%- else %}


-- First run. Select and filter all rows from staging.

staging_rows_to_insert as (
    select
          *
    from {{ staging_relation }} as staging_row
    where {{ staging_filter_condition }}
)

{%- endif %}


-- Final output. Return rows to append to history.

select *
from staging_rows_to_insert

{%- if order_by_expression %}
order by {{ order_by_expression }}
{%- endif %}

{%- endmacro %}
