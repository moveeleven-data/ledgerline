with

dim_customer_base as (
  {{ self_completing_dimension(
        dim_rel                = ref('ref_customer_atlas')
      , dim_key_column         = 'customer_code'
      , dim_default_key_value  = '-1'
      , rel_columns_to_exclude = ['customer_hkey']
      , fact_defs              = [ {'model': 'ref_usage_atlas', 'key': 'customer_code'} ]
  ) }}
)

select
    coalesce(
        ref.customer_hkey
      , {{ dbt_utils.generate_surrogate_key(['base.customer_code']) }}
    ) as customer_key
    
  , base.customer_code
  , coalesce(ref.customer_name, base.customer_name) as customer_name
  , coalesce(ref.country_code2, base.country_code2) as country_code
from dim_customer_base as base
left join {{ ref('ref_customer_atlas') }} as ref
  on ref.customer_code = base.customer_code
where base.customer_code is not null
