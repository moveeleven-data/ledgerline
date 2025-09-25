{# 
  latest_prior_open.sql
  -------------------------------
  Select the most recent OPEN usage row for each business key (customer, product, plan),
  strictly before a given as-of date. Used to detect subscriptions that were open 
  prior to the processing date.
#}

{% macro latest_prior_open(history_relation, as_of_date_literal) -%}

select *
from {{ history_relation }}
where usage_row_type = 'OPEN'
  and report_date    < {{ as_of_date_literal }}

qualify row_number() over (
          partition by
                customer_code
              , product_code
              , plan_code
          order by
                report_date desc
              , load_ts_utc desc
       ) = 1

{%- endmacro %}