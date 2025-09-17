select
    currency_hkey     as currency_key
  , currency_code     as currency_code
  , currency_num_code as currency_num_code
  , decimal_digits    as decimal_digits
  , currency_name     as currency_name
  , locations         as locations
from {{ ref('REF_CURRENCY_ABC_BANK') }}
where currency_code <> '-1'