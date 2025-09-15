select
  security_hkey as security_key,
  security_code as security_code,
  security_name as security_name,
  sector_name   as sector_name,
  industry_name as industry_name,
  country_code  as country_code,
  exchange_code as exchange_code
from {{ ref('REF_SECURITY_INFO_ABC_BANK') }}