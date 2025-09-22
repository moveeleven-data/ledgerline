{{ save_history(
      input_rel   = ref('stg_atlas_crm_customer_info')
    , key_column  = 'CUSTOMER_HKEY'
    , diff_column = 'CUSTOMER_HDIFF'
) }}
