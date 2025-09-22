with usage_norm as (
  select
        report_date::date                                as report_date
      , cast(units_used as number(38,0))                 as units_used
      , cast(included_units as number(38,0))             as included_units
      , greatest(units_used - included_units, 0)         as overage_units
      , coalesce(customer_code, '-1')                    as customer_code_nk
      , coalesce(product_code,  '-1')                    as product_code_nk
      , coalesce(plan_code,     '-1')                    as plan_code_nk
  from {{ ref('ref_usage_atlas') }}
)

, rate_card as (
  select
        product_code
      , plan_code
      , price_date
      , unit_price
  from {{ ref('stg_atlas_pricing_rate_card_daily') }}
)

, usage_priced as (
  select
        u.report_date
      , u.units_used
      , u.included_units
      , u.overage_units
      , rc.unit_price
      , u.customer_code_nk
      , u.product_code_nk
      , u.plan_code_nk
  from usage_norm u
  left join rate_card rc
    on  rc.product_code = u.product_code_nk
    and rc.plan_code    = u.plan_code_nk
    and rc.price_date   = u.report_date
)

, usage_enriched as (
  select
        dim_customer.customer_key         as customer_key
      , dim_product.product_key           as product_key
      , dim_plan.plan_key                 as plan_key
      , up.report_date                    as report_date
      , up.units_used                     as units_used
      , up.included_units                 as included_units
      , up.overage_units                  as overage_units

      , coalesce(up.unit_price, 0)                                         as unit_price
      , (up.units_used * coalesce(up.unit_price, 0))                       as billed_value
      , (up.included_units * coalesce(up.unit_price, 0))                   as included_value
      , ((up.units_used - up.included_units) * coalesce(up.unit_price, 0)) as margin_value

      , case
            when (up.units_used * coalesce(up.unit_price, 0)) > 0
              then ((up.units_used - up.included_units) * coalesce(up.unit_price, 0))
                   / (up.units_used * coalesce(up.unit_price, 0))
            else 0
        end as margin_pct

  from usage_priced up

  join {{ ref('dim_customer') }} as dim_customer
    on dim_customer.customer_code = up.customer_code_nk

  join {{ ref('dim_product') }} as dim_product
    on dim_product.product_code = up.product_code_nk

  join {{ ref('dim_plan') }} as asdim_plan
    on dim_plan.plan_code = up.plan_code_nk
)

select
    customer_key
  , product_key
  , plan_key
  , report_date
  , units_used
  , included_units
  , overage_units
  , unit_price
  , billed_value
  , included_value
  , margin_value
  , margin_pct
from usage_enriched
