{{ save_history(
      staging_relation   = ref('stg_atlas_crm_customer_info')
    , surrogate_key_column  = 'customer_hkey'
    , version_hash_column = 'customer_hdiff'
) }}
