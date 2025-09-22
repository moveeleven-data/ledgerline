with

current_from_history as (
    {{ current_from_history(
          history_rel = ref('hist_atlas_catalog_plan_info')
        , key_column  = 'PLAN_HKEY'
    ) }}
)

select
    plan_hkey
  , plan_code
  , plan_name
  , product_code
  , billing_period
  , record_source
  , load_ts_utc
from current_from_history
