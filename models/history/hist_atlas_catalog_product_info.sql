{{ save_history(
      input_rel   = ref('stg_atlas_catalog_product_info')
    , key_column  = 'product_hkey'
    , diff_column = 'product_hdiff'
) }}
