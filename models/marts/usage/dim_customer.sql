select
    customer_hkey as customer_key
  , customer_code
  , customer_name
  , country_code2 as country_code
from {{ ref('ref_customer_atlas') }}
