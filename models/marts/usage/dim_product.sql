/**
 * dim_product.sql
 * ---------------
 * Pass-through of the refined product dimension.
 * Grain: one row per product_key.
 */

select
      product_hkey as product_key
    , product_code
    , product_name
    , category
from {{ ref('ref_product_atlas') }}
