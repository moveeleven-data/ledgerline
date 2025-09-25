with

current_from_history as (
    {{ current_from_history(
          history_relation = ref('hist_atlas_crm_customer_info')
        , key_column  = 'customer_hkey'
    ) }}
)

select
    customer_hkey
  , customer_code
  , customer_name
  , country_code2
  , record_source
  , load_ts_utc
from current_from_history
