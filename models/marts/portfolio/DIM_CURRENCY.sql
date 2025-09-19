select
    currency_hkey                            as currency_key
  , currency_code                            as currency_code
  , cast(currency_num_code as number(38,0))  as currency_num_code
  , cast(decimal_digits    as number(38,0))  as decimal_digits
  , currency_name                            as currency_name
  , locations                                as locations
from {{ ref('REF_CURRENCY_ABC_BANK') }}
where currency_code <> '-1'