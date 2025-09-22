{{ save_history(
      input_rel   = ref('stg_atlas_crm_customer_info')
    , key_column  = 'customer_hkey'
    , diff_column = 'customer_hdiff'
) }}
