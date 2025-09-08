with
current_from_snapshot as (
    {{ current_from_snapshot(
          snsh_ref = ref('SNSH_ABC_BANK_SECURITY_INFO')
       ) }}
)
select *
from current_from_snapshot