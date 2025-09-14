with
current_from_snapshot as (
    {{ current_from_history(
          history_rel = ref('HIST_ABC_BANK_EXCHANGE_INFO'),
          key_column = 'EXCHANGE_HKEY',
       ) }}
)
select *
from current_from_snapshot