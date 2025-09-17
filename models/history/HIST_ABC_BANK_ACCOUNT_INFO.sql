{{ save_history(
    input_rel = ref('STG_ABC_BANK_ACCOUNT_INFO')
  , key_column = 'ACCOUNT_HKEY'
  , diff_column ='ACCOUNT_HDIFF'
) }}