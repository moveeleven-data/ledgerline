{% macro as_sql_list(hashed_fields_list) -%}
    {{ hashed_fields_list | join(', ') }}
{%- endmacro %}

{% test no_hash_collisions(model, column_name, hashed_fields) %}

with

all_tuples as (
    select
        distinct {{ column_name }} as HASH,
        {{ hashed_fields }}
    from {{ model }}
),

validation_errors as (
    select
        HASH,
        count(*)
    from all_tuples
    group by HASH
    having count(*) > 1
)

select * from validation_errors

{%- endtest %}