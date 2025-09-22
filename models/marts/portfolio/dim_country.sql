select
    {{ dbt_utils.generate_surrogate_key(['country_code2']) }} as country_key
  , country_code2 as country_code
  , country_name
from {{ ref('REF_COUNTRY_ATLAS') }}
