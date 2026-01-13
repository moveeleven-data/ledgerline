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

select
      product_hkey as product_key
    , product_code
    , product_name
    , category
    , record_source
    , load_ts_utc
from {{ ref('stg_atlas_catalog_product_info') }}
