with

current_from_history as (
    {{ current_from_history(
          history_rel = ref('HIST_ABC_BANK_ACCOUNT_INFO'),
          key_column = 'ACCOUNT_HKEY',
       ) }}
)

select *
from current_from_history