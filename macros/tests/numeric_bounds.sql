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

{% test numeric_bounds(model, column_name, min_value=None, max_value=None) %}

{% set has_min = (min_value is not none) %}   -- True if a minimum bound was supplied.
{% set has_max = (max_value is not none) %}   -- True if a maximum bound was supplied.
{% set col = adapter.quote(column_name) %}    -- The column name, quoted safely for the warehouse.

select *
from {{ model }}
where

  (
    {% if has_min %}
      {{ col }} < {{ min_value }}  -- value is smaller than the minimum allowed
    {% else %}
      false   -- no minimum bound specified
    {% endif %}
  )

  or

  (
    {% if has_max %}
      {{ col }} > {{ max_value }}  -- value is larger than the maximum allowed
    {% else %}
      false   -- no maximum bound specified
    {% endif %}
  )

{% endtest %}