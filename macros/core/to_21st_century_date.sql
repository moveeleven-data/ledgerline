{% macro to_21st_century_date(col) %}
(
    try_to_date(
        case
            -- Fix dates like 0021-04-09 by replacing leading "00" with "20"
            when substr(trim({{ col }}::varchar), 1, 2) = '00'
            then '20' || substr(trim({{ col }}::varchar), 3)
            
            -- Otherwise, keep the value as is
            else trim({{ col }}::varchar)
        end
        
     , 'YYYY-MM-DD'
    )
)
{% endmacro %}