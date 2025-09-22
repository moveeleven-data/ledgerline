{{ save_history(
      input_rel   = ref('stg_atlas_ref_country_info')
    , key_column  = 'COUNTRY_HKEY'
    , diff_column = 'COUNTRY_HDIFF'
) }}
