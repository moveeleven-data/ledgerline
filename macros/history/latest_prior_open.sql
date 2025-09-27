/**
* latest_prior_open.sql
* -------------------------------
* Detects subscriptions that were open prior to the processing date.
*
* Selects the most recent OPEN usage row for each composite
* business key (customer, product, plan) strictly before a given as-of date.
*
* - Used by our Usage fact's HIST model to identify which subscriptions were
*   already open coming into the current day.
*
* - These “prior open” rows are compared against today’s feed to detect
*   missing keys and generate synthetic CLOSE rows when a subscription
*   silently drops out.
**/

{% macro latest_prior_open(history_relation, as_of_date_literal) -%}

select
    *
from {{ history_relation }}
where
      usage_row_type = 'OPEN'
  and report_date < {{ as_of_date_literal }}

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