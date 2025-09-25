with

dim_currency_base as (
  {{ self_completing_dimension(
        dim_rel                = ref('ref_currency_atlas')
      , dim_key_column         = 'currency_code'
      , dim_default_key_value  = '-1'
      , rel_columns_to_exclude = ['currency_hkey']
      , fact_defs              = []   -- no fact model in this mart carries currency_code
  ) }}
)

select
    coalesce(
        ref.currency_hkey
      , {{ dbt_utils.generate_surrogate_key(['base.currency_code']) }}
    ) as currency_key

  , base.currency_code
  , cast(coalesce(ref.decimal_digits, base.decimal_digits) as number(38,0)) as decimal_digits
  , coalesce(ref.currency_name, base.currency_name) as currency_name
from dim_currency_base as base
left join {{ ref('ref_currency_atlas') }} as ref
  on ref.currency_code = base.currency_code
where base.currency_code is not null
