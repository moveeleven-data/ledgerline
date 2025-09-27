/**
 * dim_country.sql
 * ---------------
 * Country is mostly a closed domain, but customers can surface new codes
 * before the reference list is updated. We use self-completion to keep the
 * dimension joinable at all times, then prefer canonical attributes from
 * the refined country table when present.
 *
 * Design rules:
 * - Keys come from the natural code (country_code2).
 * - If a customer references a country not yet in ref_country_atlas, clone
 *   the default row to keep joins working until the reference is updated.
 * - Prefer refined attributes when available; otherwise carry base values.
 *
 * Note: We are not re-generating hashes for rows that already have them.
 * We provide a fallback key only for the synthetic rows the
 * self_completing_dimension macro creates.
 */

with

/* Step 1. Build the base set via self-completion.

   Start from the refined country reference and add any country codes that
   appear on customers but are missing in the dimension. */

dim_country_base as (
  {{ self_completing_dimension(
        dim_rel                = ref('ref_country_atlas')
      , dim_key_column         = 'country_code2'
      , dim_default_key_value  = '-1'
      , rel_columns_to_exclude = ['country_hkey']
      , fact_defs              = [ {'model': 'ref_customer_atlas', 'key': 'country_code2'} ]
  ) }}
)

/* Step 2. Enrich with canonical attributes and pick a stable key.

   Use the refined surrogate key when it exists. For synthetic rows created
   by the macro, fall back to a deterministic surrogate from the code. */

select
    coalesce(
        ref.country_hkey
      , {{ dbt_utils.generate_surrogate_key(["upper(base.country_code2)"]) }}
    ) as country_key
  , base.country_code2 as country_code
  , coalesce(ref.country_name, base.country_name) as country_name
  
from dim_country_base as base
left join {{ ref('ref_country_atlas') }} as ref
  on ref.country_code2 = base.country_code2
where 
    base.country_code2 is not null
