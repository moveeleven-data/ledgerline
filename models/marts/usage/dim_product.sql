/**
 * dim_product.sql
 * ---------------
 * Product is the catalog of services. Usage can reference a product before
 * the catalog refresh lands, so we self-complete from usage to prevent row
 * loss in the fact. Canonical attributes are taken from the refined product
 * table when available.
 *
 * Design rules:
 * - Keys come from product_code, with the refined surrogate key preferred.
 * - Default member ensures safe joins when inputs are incomplete.
 *
 * Note: We are not re-generating hashes for rows that already have them.
 * We provide a fallback key only for the synthetic rows the
 * self_completing_dimension macro creates.
 */

with

/* Step 1. Build the base set via self-completion.

   Start from the refined product reference and add any product codes that
   appear in usage but are missing in the dimension. */

dim_product_base as (
  {{ self_completing_dimension(
        dim_rel                = ref('ref_product_atlas')
      , dim_key_column         = 'product_code'
      , dim_default_key_value  = '-1'
      , rel_columns_to_exclude = ['product_hkey']
      , fact_defs              = [ {'model': 'ref_usage_atlas', 'key': 'product_code'} ]
  ) }}
)

/* Step 2. Enrich with canonical attributes and pick a stable key.

   Prefer the refined surrogate key when present; otherwise generate a
   deterministic surrogate from product_code. */

select
    coalesce(
        ref.product_hkey
      , {{ dbt_utils.generate_surrogate_key(["upper(base.product_code)"]) }}
    ) as product_key
  , base.product_code
  , coalesce(ref.product_name, base.product_name) as product_name
  , coalesce(ref.category,     base.category)     as category
from dim_product_base as base
left join {{ ref('ref_product_atlas') }} as ref
  on ref.product_code = base.product_code
where
    base.product_code is not null
