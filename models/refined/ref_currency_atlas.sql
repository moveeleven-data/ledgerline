with

current_from_history as (
    {{ current_from_history(
          history_relation = ref('hist_atlas_currency_info')
        , key_column  = 'currency_hkey'
    ) }}
)

select
    currency_hkey
  , currency_code
  , currency_name
  , decimal_digits
  , record_source
  , load_ts_utc
from current_from_history
