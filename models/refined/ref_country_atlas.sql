with

current_from_history as (
    {{ current_from_history(
          history_rel = ref('hist_atlas_country_info')
        , key_column  = 'country_hkey'
    ) }}
)

select
    country_hkey
  , country_code2
  , country_code3
  , country_name
  , record_source
  , load_ts_utc
from current_from_history
