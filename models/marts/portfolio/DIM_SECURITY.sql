with

dim_security as (
    /* We use self-completing dimensions because the natural key is high variance
       and not fully controlled by the seed. */

    -- union of known securities + any codes seen in facts
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
      -- Use ref key when present (otherwise a stable surrogate from the code)
        coalesce(ref.security_hkey
            , {{ dbt_utils.generate_surrogate_key(['base.security_code']) }}
        ) as security_key

      , base.security_code

      -- Prefer attributes from ref when we have them
      , coalesce(ref.security_name,   base.security_name)   as security_name
      , coalesce(ref.sector_name,     base.sector_name)     as sector_name
      , coalesce(ref.industry_name,   base.industry_name)   as industry_name
      , coalesce(ref.country_code,    base.country_code)    as country_code
      , coalesce(ref.exchange_code,   base.exchange_code)   as exchange_code
  from dim_security base
  left join {{ ref('REF_SECURITY_ABC_BANK') }} ref
    on ref.security_code = base.security_code
 where base.security_code is not null
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