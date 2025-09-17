{{ save_history(
    input_rel = ref('STG_ABC_BANK_EXCHANGE_INFO')
  , key_column = 'EXCHANGE_HKEY'
  , diff_column ='EXCHANGE_HDIFF'
) }}