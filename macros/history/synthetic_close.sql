{# 
  synthetic_close.sql
  ------------------------------------
  Build synthetic CLOSE rows for business keys that were previously OPEN but 
  are missing on the current as-of date. These are "zero-out" rows to properly 
  terminate usage spans.
#}

{% macro synthetic_close(
      prior_open_alias
    , today_keys_alias
    , as_of_date_literal
    , diff_fields_for_close
) -%}

select
      {{ dbt_utils.generate_surrogate_key([
            'prior.customer_code'
          , 'prior.product_code'
          , 'prior.plan_code'
          , "to_varchar(" ~ as_of_date_literal ~ ", 'YYYY-MM-DD')"
      ]) }}                                                          as usage_hkey
    , {{ dbt_utils.generate_surrogate_key(diff_fields_for_close) }}  as usage_hdiff
    , prior.customer_code                                            as customer_code
    , prior.product_code                                             as product_code
    , prior.plan_code                                                as plan_code
    , prior.record_source                                            as record_source
    , {{ as_of_date_literal }}                                       as report_date
    , 0                                                              as units_used
    , 0                                                              as included_units
    , '{{ run_started_at }}'::timestamp_ntz                          as load_ts_utc
    , 'CLOSE_SYNTHETIC'                                              as usage_row_type

from {{ prior_open_alias }} as prior

left join {{ today_keys_alias }} as today
       on today.customer_code = prior.customer_code
      and today.product_code  = prior.product_code
      and today.plan_code     = prior.plan_code

where today.customer_code is null

{%- endmacro %}