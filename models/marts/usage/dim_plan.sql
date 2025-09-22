select
    plan_hkey as plan_key
  , plan_code
  , plan_name
  , product_code
  , billing_period
from {{ ref('ref_plan_atlas') }}
