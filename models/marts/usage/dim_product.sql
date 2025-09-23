select
    product_hkey as product_key
  , product_code
  , product_name
  , category
from {{ ref('ref_product_atlas') }}
