with

dim_plan_base as (
  {{ self_completing_dimension(
        dim_rel                = ref('ref_plan_atlas')
      , dim_key_column         = 'plan_code'
      , dim_default_key_value  = '-1'
      , rel_columns_to_exclude = ['plan_hkey']
      , fact_defs              = [ {'model': 'ref_usage_atlas', 'key': 'plan_code'} ]
  ) }}
)

select
    coalesce(
        ref.plan_hkey
      , {{ dbt_utils.generate_surrogate_key(['base.plan_code']) }}
    ) as plan_key

  , base.plan_code
  , coalesce(ref.plan_name,       base.plan_name)       as plan_name
  , coalesce(ref.product_code,    base.product_code)    as product_code
  , coalesce(ref.billing_period,  base.billing_period)  as billing_period
from dim_plan_base as base
left join {{ ref('ref_plan_atlas') }} as ref
  on ref.plan_code = base.plan_code
where base.plan_code is not null