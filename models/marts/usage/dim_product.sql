/**
 * dim_product.sql
 * ---------------
 * Pass-through of the staging product catalog.
 * Grain: one row per product_key.
 */

select
      product_hkey as product_key
    , product_code
    , product_name
    , category
from {{ ref('stg_atlas_catalog_product_info') }}
