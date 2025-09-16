select
  country_hkey    as country_key
  , country_code2 as country_code
  , country_name  as country_name
  , region        as region
  , sub_region    as sub_region
  , iso_3166_2    as iso_code
from {{ ref('REF_COUNTRY_ABC_BANK') }}