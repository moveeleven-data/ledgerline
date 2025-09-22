with

current_from_history as (
    {{ current_from_history(
          history_rel = ref('hist_atlas_ref_currency_info')
        , key_column  = 'CURRENCY_HKEY'
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
