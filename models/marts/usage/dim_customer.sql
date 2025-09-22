with

dim_scaffold as (
  {{ self_completing_dimension(
        dim_rel                = ref('ref_customer_atlas')
      , dim_key_column         = 'customer_hkey'
      , rel_columns_to_exclude = ['record_source','load_ts_utc']
      , fact_defs              = [ {'model':'fact_usage', 'key':'customer_key'} ]
  ) }}
)

select
    customer_hkey as customer_key
  , customer_code
  , customer_name
  , country_code2 as country_code
from dim_scaffold
