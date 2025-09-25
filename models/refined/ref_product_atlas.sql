with

current_from_history as (
    {{ current_from_history(
          history_relation = ref('hist_atlas_catalog_product_info')
        , key_column  = 'product_hkey'
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
