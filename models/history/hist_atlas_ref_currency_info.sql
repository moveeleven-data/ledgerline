{{ save_history(
      input_rel   = ref('stg_atlas_ref_currency_info')
    , key_column  = 'currency_hkey'
    , diff_column = 'currency_hdiff'
) }}
