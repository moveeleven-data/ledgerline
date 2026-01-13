/**
 * ref_product_atlas.sql
 * ----------------------------------
 * Product dimension for the star schema.
 *
 * Purpose:
 * - Expose core product attributes (code, name, category) from the staging layer.
 * - Alias the surrogate key (`product_hkey`) to `product_key` for consistent naming.
 *
 * Grain:
 * - One row per product_key (from staging).
 */

select
      product_hkey as product_key
    , product_code
    , product_name
    , category
    , record_source
    , load_ts_utc
from {{ ref('stg_atlas_catalog_product_info') }}
