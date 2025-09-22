{{ save_history(
      input_rel   = ref('stg_atlas_catalog_product_info')
    , key_column  = 'PRODUCT_HKEY'
    , diff_column = 'PRODUCT_HDIFF'
) }}
