with
current_from_snapshot as (
    {{ current_from_history(
          history_rel = ref('HIST_ABC_BANK_CURRENCY_INFO'),
          key_column = 'CURRENCY_HKEY',
       ) }}
)
select *
from current_from_snapshot