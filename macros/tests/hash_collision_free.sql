{% test hash_collision_free(model, column_name, hashed_fields) %}

with

all_tuples as (
  select distinct
    {{ column_name }} as hash,
    {{ hashed_fields | join(', ') }}
  from {{ model }}
),

validation_errors as (
  select hash, count(*) as cnt
  from all_tuples
  group by hash
  having cnt > 1
)

select * from validation_errors

{% endtest %}
