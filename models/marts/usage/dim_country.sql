select
    country_hkey as country_key
  , country_code2 as country_code
  , country_name
from {{ ref('ref_country_atlas') }}
