with

dim_country_base as (
  {{ self_completing_dimension(
        dim_rel                = ref('ref_country_atlas')
      , dim_key_column         = 'country_code2'
      , dim_default_key_value  = '-1'
      , rel_columns_to_exclude = ['country_hkey']
      , fact_defs              = [ {'model': 'ref_customer_atlas', 'key': 'country_code2'} ]
  ) }}
)

select
    coalesce(
        ref.country_hkey
      , {{ dbt_utils.generate_surrogate_key(['base.country_code2']) }}
    ) as country_key

  , base.country_code2 as country_code
  , coalesce(ref.country_name, base.country_name) as country_name
from dim_country_base as base
left join {{ ref('ref_country_atlas') }} as ref
  on ref.country_code2 = base.country_code2
where base.country_code2 is not null
