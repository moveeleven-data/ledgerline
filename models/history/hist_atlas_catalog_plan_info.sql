{{ save_history(
      staging_relation      = ref('stg_atlas_catalog_plan_info')
    , surrogate_key_column  = 'plan_hkey'
    , version_hash_column   = 'plan_hdiff'
) }}
