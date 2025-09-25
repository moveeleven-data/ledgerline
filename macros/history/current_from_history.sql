/**
 * current_from_history.sql
 * ------------------------
 * Returns the latest row per key from a history relation.
 *
 * Behavior:
 * - Ranks rows in the history table by load timestamp (descending).
 * - For each key, keeps only the most recent row (row_number = 1).
 * - If a selection expression is provided, returns only that column;
 *   otherwise, returns the full row minus the row_number helper.
 *
 * Usage:
 * Typically called inside save_history:
 *
 *     {{ current_from_history(
 *           history_relation      = this
 *         , key_column            = surrogate_key_column
 *         , selection_expression  = version_hash_column
 *     ) }}
 */

{% macro current_from_history(
      history_relation
    , key_column
    , selection_expression = none
    , load_timestamp_column = 'LOAD_TS_UTC'
    , history_filter_condition = 'true'
) -%}

with ranked_history as (
    select
          *
        , row_number() over (
              partition by {{ key_column }}
              order by {{ load_timestamp_column }} desc
          ) as row_rank
    from {{ history_relation }}
    where {{ history_filter_condition }}
)

{% if selection_expression %}

-- Case 1: A selection_expression was provided.
-- Return only that expression from the most recent row per key.
-- Example: return just the version hash to check for changes.

select {{ selection_expression }}
from ranked_history
where row_rank = 1

{% else %}

-- Case 2: No selection_expression was provided.
-- Return the entire most recent row per key.
-- Exclude the helper column (row_rank).

select * exclude (row_rank)
from ranked_history
where row_rank = 1

{% endif %}

{%- endmacro %}
