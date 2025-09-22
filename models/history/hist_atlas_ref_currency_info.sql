{{ save_history(
      input_rel   = ref('stg_atlas_ref_currency_info')
    , key_column  = 'CURRENCY_HKEY'
    , diff_column = 'CURRENCY_HDIFF'
) }}
