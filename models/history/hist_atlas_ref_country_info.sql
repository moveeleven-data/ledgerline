{{ save_history(
      input_rel   = ref('stg_atlas_ref_country_info')
    , key_column  = 'country_hkey'
    , diff_column = 'country_hdiff'
) }}
