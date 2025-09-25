{{ save_history(
      staging_relation   = ref('stg_atlas_catalog_product_info')
    , surrogate_key_column  = 'product_hkey'
    , version_hash_column = 'product_hdiff'
) }}
