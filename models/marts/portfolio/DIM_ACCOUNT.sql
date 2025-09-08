select
  account_hkey          as account_key,
  account_code          as account_code,
  account_currency_code as account_currency_code
from {{ ref('REF_ABC_BANK_ACCOUNT_INFO') }}