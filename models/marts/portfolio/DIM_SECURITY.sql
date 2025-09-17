with

dim_security as (
    {{ self_completing_dimension(
        dim_rel = ref('REF_SECURITY_ABC_BANK')
      , dim_key_column = 'SECURITY_CODE'
      , dim_default_key_value = '-1'
      , rel_columns_to_exclude = ['SECURITY_HKEY','SECURITY_HDIFF']
      , fact_defs = [ {'model': 'REF_POSITION_ABC_BANK', 'key': 'SECURITY_CODE'} ]
    ) }}
)

, dim_security_enriched as (
  select
      src.security_hkey       as security_key
    , base.security_code      as security_code
    , base.security_name      as security_name
    , base.sector_name        as sector_name
    , base.industry_name      as industry_name
    , base.country_code       as country_code
    , base.exchange_code      as exchange_code
  from dim_security base
  left join {{ ref('REF_SECURITY_ABC_BANK') }} src
    on src.security_code = base.security_code
  where base.security_code <> '-1'
)

select
    security_key
  , security_code
  , security_name
  , sector_name
  , industry_name
  , country_code
  , exchange_code
from dim_security_enriched