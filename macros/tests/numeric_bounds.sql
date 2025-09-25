/**
 * numeric_bounds.sql
 * ------------------
 * Checks that a numeric column stays within optional min/max limits.
 *
 * Params:
 * - model:       table/view under test
 * - column_name: column to check
 * - min_value:   flag rows below this (if set)
 * - max_value:   flag rows above this (if set)
 *
 * Returns rows outside the allowed range. Useful for catching
 * negative usage, impossible prices, or other out-of-bounds values.
 */

{% test numeric_bounds(
      model
    , numeric_column
    , min_allowed_value=None
    , max_allowed_value=None
) %}

{% set has_min_bound = (min_allowed_value is not none) %}   -- True if a minimum bound was supplied
{% set has_max_bound = (max_allowed_value is not none) %}   -- True if a maximum bound was supplied
{% set quoted_column = adapter.quote(numeric_column) %}     -- Column name, safely quoted for the warehouse

select
    *
from {{ model }}
where

  (
    {% if has_min_bound %}
      {{ quoted_column }} < {{ min_allowed_value }}   -- value is smaller than the minimum allowed
    {% else %}
      false   -- no minimum bound specified
    {% endif %}
  )

  or

  (
    {% if has_max_bound %}
      {{ quoted_column }} > {{ max_allowed_value }}   -- value is larger than the maximum allowed
    {% else %}
      false   -- no maximum bound specified
    {% endif %}
  )

{% endtest %}