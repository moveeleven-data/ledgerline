{{ save_history(
      input_rel   = ref('stg_atlas_catalog_plan_info')
    , key_column  = 'plan_hkey'
    , diff_column = 'plan_hdiff'
) }}
