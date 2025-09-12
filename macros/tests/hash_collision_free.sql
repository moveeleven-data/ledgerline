{% test hash_collision_free(model, column_name, hashed_fields) %}
{% if hashed_fields is string %}
  {% set fields = [hashed_fields] %}
{% elif hashed_fields is sequence %}
  {% set fields = hashed_fields %}
{% else %}
  {% set fields = [] %}
{% endif %}

{% set parts = [ column_name ~ " as hash" ] + (fields | map('trim') | list) %}
{% set select_list = parts | join(', ') %}

with

all_tuples as (
  select distinct {{ select_list }}
  from {{ model }}
),

validation_errors as (
  select hash, count(*) as cnt
  from all_tuples
  group by hash
  having count(*) > 1
)

select * from validation_errors

{% endtest %}
