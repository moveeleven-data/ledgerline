select
    {{ dbt_utils.generate_surrogate_key(['product_code']) }} as product_key
     , product_code
     , product_name
     , category
from {{ ref('ref_product_atlas') }}
