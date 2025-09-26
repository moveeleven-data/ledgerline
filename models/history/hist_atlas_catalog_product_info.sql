/**
 * hist_atlas_catalog_product_info.sql
 * -----------------------------------
 * History table for product catalog.
 *
 * Purpose:
 * - Track attribute changes for products (SCD2).
 * - Ensure stable surrogate key (product_hkey).
 * - Detect attribute updates via product_hdiff.
 */

{{ save_history(
      staging_relation      = ref('stg_atlas_catalog_product_info')
    , surrogate_key_column  = 'product_hkey'
    , version_hash_column   = 'product_hdiff'
) }}
