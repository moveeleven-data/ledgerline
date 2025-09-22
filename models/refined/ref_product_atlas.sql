with

current_from_history as (
    {{ current_from_history(
          history_rel = ref('hist_atlas_catalog_product_info')
        , key_column  = 'PRODUCT_HKEY'
    ) }}
)

select
    product_hkey
  , product_code
  , product_name
  , category
  , record_source
  , load_ts_utc
from current_from_history
