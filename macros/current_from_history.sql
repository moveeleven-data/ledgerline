{% macro current_from_history(
    history_rel,
    key_column,
    selection_expr = none,
    load_ts_column = 'LOAD_TS_UTC',
    history_filter_expr = 'true'
) -%}

with

ranked as (
    select
        *,
        row_number() over (
            partition by {{ key_column }}
            order by {{ load_ts_column }} desc
        ) as rn
    from {{ history_rel }}
    where {{ history_filter_expr }}
)

{# If a selection_expr is provided (used by save_history), return just that column #}
{%- if selection_expr %}
select {{ selection_expr }}
from ranked
where rn = 1

{# Otherwise, return the entire current row per key (what your book’s snippet expects) #}
{%- else %}
select *
from ranked
where rn = 1
{%- endif %}

{%- endmacro %}
