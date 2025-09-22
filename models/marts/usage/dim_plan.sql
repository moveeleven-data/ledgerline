with

dim_scaffold as (
  {{ self_completing_dimension(
        dim_rel                = ref('ref_plan_atlas')
      , dim_key_column         = 'plan_hkey'
      , rel_columns_to_exclude = ['record_source','load_ts_utc']
      , fact_defs              = [ {'model':'fact_usage', 'key':'plan_key'} ]
  ) }}
)

select
    plan_hkey as plan_key
  , plan_code
  , plan_name
  , product_code
  , billing_period
from dim_scaffold
