{% macro current_from_history(
    history_rel,
    key_column,
    selection_expr,
    load_ts_column = 'LOAD_TS_UTC',
    history_filter_expr = 'true'
) -%}

with ranked as (
    select
        {{ key_column }}       as _key,
        {{ selection_expr }}   as _selection_expr,
        {{ load_ts_column }}   as _load_ts,
        row_number() over (
            partition by {{ key_column }}
            order by {{ load_ts_column }} desc
        ) as rn
    from {{ history_rel }}
    where {{ history_filter_expr }}
)
select
    _selection_expr as {{ selection_expr }}
from ranked
where rn = 1

{%- endmacro %}
