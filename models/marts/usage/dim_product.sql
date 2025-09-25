with

dim_product_base as (
  {{ self_completing_dimension(
        dim_rel                = ref('ref_product_atlas')
      , dim_key_column         = 'product_code'
      , dim_default_key_value  = '-1'
      , rel_columns_to_exclude = ['product_hkey']
      , fact_defs              = [ {'model': 'ref_usage_atlas', 'key': 'product_code'} ]
  ) }}
)

select
    coalesce(
        ref.product_hkey
      , {{ dbt_utils.generate_surrogate_key(['base.product_code']) }}
    ) as product_key
    
  , base.product_code
  , coalesce(ref.product_name, base.product_name) as product_name
  , coalesce(ref.category,     base.category)     as category
from dim_product_base as base
left join {{ ref('ref_product_atlas') }} as ref
  on ref.product_code = base.product_code
where base.product_code is not null