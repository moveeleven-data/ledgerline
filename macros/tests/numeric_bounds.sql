{% test numeric_bounds(model, column_name, min_value=None, max_value=None) %}

select *
from {{ model }}
where
    {% if min_value is not none %}
        {{ adapter.quote(column_name) }} < {{ min_value }}
    {% else %}
        1 = 0
    {% endif %}

    {% if min_value is not none and max_value is not none %}
        or
    {% endif %}

    {% if max_value is not none %}
        {{ adapter.quote(column_name) }} > {{ max_value }}
    {% endif %}

{% endtest %}
