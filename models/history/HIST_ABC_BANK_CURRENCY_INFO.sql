{{ save_history(
    input_rel = ref('STG_ABC_BANK_CURRENCY_INFO')
  , key_column = 'CURRENCY_HKEY'
  , diff_column ='CURRENCY_HDIFF'
) }}