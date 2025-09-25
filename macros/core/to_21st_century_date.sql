/**
 * to_21st_century_date.sql
 * ------------------------
 * Normalize a date column so that invalid leading "00" years
 * (e.g. "0021-04-09") are rewritten into valid 21st-century dates
 * (e.g. "2021-04-09").
 *
 * Usage:
 *     {{ to_21st_century_date('report_date') }}
 */

{% macro to_21st_century_date(col) %}

(
    try_to_date(
        case
            -- If the year starts with "00", rewrite it to start with "20"
            when substr(
                     trim({{ col }}::varchar)  -- the cleaned string version of the column
                   , 1                         -- starting at the first character
                   , 2                         -- take two characters
                 ) = '00'
            then '20' || substr(
                           trim({{ col }}::varchar)  -- same cleaned string
                         , 3                         -- everything from the 3rd character onward
                       )

            -- Otherwise, keep the cleaned original value
            else trim({{ col }}::varchar)
        end
     , 'YYYY-MM-DD'
    )
)

{% endmacro %}