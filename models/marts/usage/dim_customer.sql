/**
 * dim_customer.sql
 * ----------------
 * Customer is a conformed dimension keyed by customer_code. We self-complete
 * from usage so that fact rows never drop when a new customer appears before
 * the refined customer table is updated.
 *
 * Design rules:
 * - Keys come from the natural code (customer_code) with a stable surrogate
 *   key preferred from the refined table when available.
 * - Default member guarantees referential integrity for incomplete inputs.
 */

with

/* Step 1. Build the base set via self-completion.

   Start from the refined customer reference and add any customer codes that
   appear in usage but are missing in the dimension. */

dim_customer_base as (
  {{ self_completing_dimension(
        dim_rel                = ref('ref_customer_atlas')
      , dim_key_column         = 'customer_code'
      , dim_default_key_value  = '-1'
      , rel_columns_to_exclude = ['customer_hkey']
      , fact_defs              = [ {'model': 'ref_usage_atlas', 'key': 'customer_code'} ]
  ) }}
)

/* Step 2. Enrich with canonical attributes and pick a stable key.

   Prefer the refined surrogate key when present. Otherwise generate a
   deterministic surrogate from the business key. */

select
    coalesce(
        ref.customer_hkey
      , {{ dbt_utils.generate_surrogate_key(["upper(base.customer_code)"]) }}
    ) as customer_key
  , base.customer_code
  , coalesce(ref.customer_name, base.customer_name) as customer_name
  , coalesce(ref.country_code2, base.country_code2) as country_code  
from dim_customer_base as base
left join {{ ref('ref_customer_atlas') }} as ref
  on ref.customer_code = base.customer_code
where 
    base.customer_code is not null
