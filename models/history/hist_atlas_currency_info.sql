{{ save_history(
      staging_relation   = ref('stg_atlas_currency_info')
    , surrogate_key_column  = 'currency_hkey'
    , version_hash_column = 'currency_hdiff'
) }}
