select
    currency_hkey as currency_key
  , currency_code
  , cast(decimal_digits as number(38,0)) as decimal_digits
  , currency_name
from {{ ref('ref_currency_atlas') }}
