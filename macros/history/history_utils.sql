{# latest prior OPEN per key, strictly before an as-of date #}
{% macro latest_prior_open_sql(history_rel, as_of_date_literal) -%}
select
    *
from {{ history_rel }}
where position_row_type = 'OPEN'
  and report_date       < {{ as_of_date_literal }}
qualify row_number() over (
          partition by position_hkey
          order by
              report_date desc
            , load_ts_utc desc
       ) = 1
{%- endmacro %}

{# build the SELECT that generates synthetic CLOSE rows #}
{% macro synthetic_close_select_sql(prior_alias, today_keys_alias, as_of_date_literal, diff_fields_close) -%}
select
    prior.position_hkey                                                as position_hkey
  , {{ dbt_utils.generate_surrogate_key(diff_fields_close) }}          as position_hdiff
  , prior.account_code                                                 as account_code
  , prior.security_code                                                as security_code
  , prior.security_name                                                as security_name
  , prior.exchange_code                                                as exchange_code
  , prior.currency_code                                                as currency_code
  , prior.record_source                                                as record_source
  , {{ as_of_date_literal }}                                           as report_date
  , 0                                                                  as quantity
  , 0                                                                  as cost_base
  , 0                                                                  as position_value
  , '{{ run_started_at }}'::timestamp_ntz                              as load_ts_utc
  , 'CLOSE_SYNTHETIC'                                                  as position_row_type
from {{ prior_alias }} prior
left join {{ today_keys_alias }} tk
  on tk.position_hkey = prior.position_hkey
where tk.position_hkey is null
{%- endmacro %}
