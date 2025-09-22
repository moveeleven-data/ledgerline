with

dim_scaffold as (
  {{ self_completing_dimension(
        dim_rel                = ref('ref_product_atlas')
      , dim_key_column         = 'product_hkey'
      , rel_columns_to_exclude = ['record_source','load_ts_utc']
      , fact_defs              = [ {'model':'fact_usage', 'key':'product_key'} ]
  ) }}
)

select
    product_hkey as product_key
  , product_code
  , product_name
  , category
from dim_scaffold
