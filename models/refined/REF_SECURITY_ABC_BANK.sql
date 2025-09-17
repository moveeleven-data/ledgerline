with

current_from_history as (
    {{ current_from_history(
          history_rel = ref('HIST_ABC_BANK_SECURITY_INFO')
        , key_column = 'SECURITY_HKEY'
    ) }}
)

select * from current_from_history