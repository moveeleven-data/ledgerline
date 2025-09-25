/**
 * hash_collision_free.sql
 * -----------------------
 * Validates that a hash column is collision-free for its underlying fields.
 *
 * Usage:
 * Applied in history.yml to:
 * - usage_hkey  (surrogate key over customer_code, product_code, plan_code, report_date bucket)
 * - usage_hdiff (version hash over identifying fields and units, includes report_date)
 */

{% test hash_collision_free(
      model
    , hash_column
    , source_columns
) %}

with

all_tuples as (
    select distinct
          {{ hash_column }} as hash
        , {{ source_columns | join(', ') }}
    from {{ model }}
)

, validation_errors as (
    select
          hash
        , count(*) as row_count
    from all_tuples
    group by hash
    having count(*) > 1
)

select
    *
from validation_errors

{% endtest %}
