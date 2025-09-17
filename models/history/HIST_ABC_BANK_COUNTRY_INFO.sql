{{ save_history(
    input_rel = ref('STG_ABC_BANK_COUNTRY_INFO')
  , key_column = 'COUNTRY_HKEY'
  , diff_column ='COUNTRY_HDIFF'
) }}