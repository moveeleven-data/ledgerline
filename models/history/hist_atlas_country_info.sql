{{ save_history(
      staging_relation      = ref('stg_atlas_country_info')
    , surrogate_key_column  = 'country_hkey'
    , version_hash_column   = 'country_hdiff'
) }}
