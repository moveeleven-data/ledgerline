/**
 * fact_usage.sql
 * --------------
 * Daily usage fact (priced) at the grain:
 *   customer_key × product_key × plan_key × report_date.
 *
 * Notes:
 * - No joins to dimensions just to fetch keys.
 * - Keys are computed deterministically from NKs (same recipe as STG/REF).
 */

with priced as (
    select
          report_date
        , customer_code_nk
        , product_code_nk
        , plan_code_nk
        , currency_code_nk
        , units_used
        , included_units
        , overage_units
        , coalesce(unit_price, 0) as unit_price
    from {{ ref('int_fact_usage_priced') }}
)

select
      {{ dbt_utils.generate_surrogate_key(['upper(customer_code_nk)']) }}  as customer_key
    , {{ dbt_utils.generate_surrogate_key(['upper(product_code_nk)'])  }}  as product_key
    , {{ dbt_utils.generate_surrogate_key(['upper(plan_code_nk)'])     }}  as plan_key
    , {{ dbt_utils.generate_surrogate_key(['upper(currency_code_nk)']) }}  as currency_key

    , report_date
    , units_used
    , included_units
    , overage_units
    , unit_price
    , (units_used * unit_price)     as billed_value
    , (included_units * unit_price) as included_value
    , (overage_units  * unit_price) as overage_value

    , case
          when (units_used * unit_price) > 0
              then (overage_units * unit_price) / (units_used * unit_price)
          else 0
      end as overage_share
from priced
