/**
 * dim_plan.sql
 * ------------
 * Plan is the subscription catalog by product. Usage can reference a plan
 * before the catalog is refreshed, so we self-complete from usage to maintain
 * joinability. Canonical attributes are taken from the refined plan table.
 *
 * Design rules:
 * - Keys come from plan_code with a stable surrogate key preferred from the
 *   refined table when present.
 * - Default member keeps joins valid when inputs are incomplete.
 *
 * Note: We are not re-generating hashes for rows that already have them.
 * We provide a fallback key only for the synthetic rows the
 * self_completing_dimension macro creates.
 */

with

/* Step 1. Build the base set via self-completion.

   Start from the refined plan reference and add any plan codes that appear
   in usage but are missing in the dimension. */

dim_plan_base as (
  {{ self_completing_dimension(
        dim_rel                = ref('ref_plan_atlas')
      , dim_key_column         = 'plan_code'
      , dim_default_key_value  = '-1'
      , rel_columns_to_exclude = ['plan_hkey']
      , fact_defs              = [ {'model': 'ref_usage_atlas', 'key': 'plan_code'} ]
  ) }}
)

/* Step 2. Enrich with canonical attributes and pick a stable key.

   Prefer the refined surrogate key when present; otherwise generate a
   deterministic surrogate from plan_code. */

select
    coalesce(
        ref.plan_hkey
      , {{ dbt_utils.generate_surrogate_key(["upper(base.plan_code)"]) }}
    ) as plan_key

  , base.plan_code
  , coalesce(ref.plan_name,       base.plan_name)       as plan_name
  , coalesce(ref.product_code,    base.product_code)    as product_code
  , coalesce(ref.billing_period,  base.billing_period)  as billing_period
  
from dim_plan_base as base
left join {{ ref('ref_plan_atlas') }} as ref
  on ref.plan_code = base.plan_code
where base.plan_code is not null
