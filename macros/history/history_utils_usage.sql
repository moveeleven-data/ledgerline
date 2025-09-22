{# Latest prior OPEN per BUSINESS KEY (customer, product, plan), strictly before an as-of date #}
{% macro latest_prior_usage_open_sql(history_rel, as_of_date_literal) -%}
select *
from {{ history_rel }}
where usage_row_type = 'OPEN'
  and report_date    < {{ as_of_date_literal }}
qualify row_number() over (
          partition by
                customer_code
              , product_code
              , plan_code
          order by
                report_date  desc
              , load_ts_utc  desc
       ) = 1
{%- endmacro %}


{# Build the SELECT that generates synthetic CLOSE rows. Compare on BUSINESS KEY. #}
{% macro synthetic_close_usage_select_sql(prior_alias, today_keys_alias, as_of_date_literal, diff_fields_close) -%}
select
      {{ dbt_utils.generate_surrogate_key([
            'prior.customer_code'
          , 'prior.product_code'
          , 'prior.plan_code'
          , "to_varchar(" ~ as_of_date_literal ~ ", 'YYYY-MM-DD')"
      ]) }}                                                     as usage_hkey
    , {{ dbt_utils.generate_surrogate_key(diff_fields_close) }} as usage_hdiff
    , prior.customer_code                                       as customer_code
    , prior.product_code                                        as product_code
    , prior.plan_code                                           as plan_code
    , prior.record_source                                       as record_source
    , {{ as_of_date_literal }}                                  as report_date
    , 0                                                         as units_used
    , 0                                                         as included_units
    , '{{ run_started_at }}'::timestamp_ntz                     as load_ts_utc
    , 'CLOSE_SYNTHETIC'                                         as usage_row_type
from {{ prior_alias }} prior
left join {{ today_keys_alias }} today_keys
  on  today_keys.customer_code = prior.customer_code
  and today_keys.product_code  = prior.product_code
  and today_keys.plan_code     = prior.plan_code
where today_keys.customer_code is null
{%- endmacro %}