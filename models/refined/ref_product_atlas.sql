/**
 * ref_product_atlas.sql
 * ----------------------------------
 * Refined dimension for products.
 *
 * Purpose:
 * - Collapse SCD history to the current product record per surrogate key.
 * - Provide stable product attributes for marts.
 *
 * Grain:
 * - One row per product_hkey.
 */

with current_from_history as (
    {{ current_from_history(
          history_relation = ref('hist_atlas_catalog_product_info')
        , key_column       = 'product_hkey'
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
