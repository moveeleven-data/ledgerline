{{ save_history(
      input_rel   = ref('stg_atlas_catalog_plan_info')
    , key_column  = 'PLAN_HKEY'
    , diff_column = 'PLAN_HDIFF'
) }}
