with

current_from_history as (
    {{ current_from_history(
          history_rel = ref('HIST_ABC_BANK_COUNTRY_INFO')
          , key_column = 'COUNTRY_HKEY'
       ) }}
)

select * from current_from_history