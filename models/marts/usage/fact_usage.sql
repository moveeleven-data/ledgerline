with

usage_norm as (
  select
        report_date::date                        as report_date
      , cast(units_used as number(38,0))         as units_used
      , cast(included_units as number(38,0))     as included_units
      , greatest(units_used - included_units, 0) as overage_units
      , coalesce(customer_code, '-1')            as customer_code_nk
      , coalesce(product_code,  '-1')            as product_code_nk
      , coalesce(plan_code,     '-1')            as plan_code_nk
  from {{ ref('ref_usage_atlas') }}
)

, price_lookup as (
  select
        stg_price.product_code
      , stg_price.plan_code
      , stg_price.price_date
      , stg_price.unit_price
      , coalesce(stg_price.currency_code, 'USD') as currency_code   -- set explicit currency
  from {{ ref('stg_atlas_price_book_daily') }} stg_price
)

, usage_priced as (
  select
        usage_norm.report_date                 as report_date
      , usage_norm.units_used                  as units_used
      , usage_norm.included_units              as included_units
      , usage_norm.overage_units               as overage_units
      , price_effective.unit_price             as unit_price
      , price_effective.currency_code          as currency_code_nk  -- carry forward
      , usage_norm.customer_code_nk            as customer_code_nk
      , usage_norm.product_code_nk             as product_code_nk
      , usage_norm.plan_code_nk                as plan_code_nk
  from usage_norm
  left join price_lookup price_effective
    on  price_effective.product_code = usage_norm.product_code_nk
    and price_effective.plan_code    = usage_norm.plan_code_nk
    and price_effective.price_date   = (
          select max(price_lookup_inner.price_date)
          from price_lookup price_lookup_inner
          where price_lookup_inner.product_code = usage_norm.product_code_nk
            and price_lookup_inner.plan_code    = usage_norm.plan_code_nk
            and price_lookup_inner.price_date  <= usage_norm.report_date
        )
)

, usage_enriched as (
  select
        dim_customer.customer_key                                              as customer_key
      , dim_product.product_key                                                as product_key
      , dim_plan.plan_key                                                      as plan_key
      , dim_currency.currency_key                                              as currency_key   -- join to closed-domain dim
      , usage_priced.report_date                                               as report_date
      , usage_priced.units_used                                                as units_used
      , usage_priced.included_units                                            as included_units
      , usage_priced.overage_units                                             as overage_units

      , coalesce(usage_priced.unit_price, 0)                                   as unit_price
      , (usage_priced.units_used      * coalesce(usage_priced.unit_price, 0))  as billed_value
      , (usage_priced.included_units  * coalesce(usage_priced.unit_price, 0))  as included_value
      , (usage_priced.overage_units   * coalesce(usage_priced.unit_price, 0))  as overage_value

      , case
            when (usage_priced.units_used * coalesce(usage_priced.unit_price, 0)) > 0
              then (usage_priced.overage_units * coalesce(usage_priced.unit_price, 0))
                   / (usage_priced.units_used * coalesce(usage_priced.unit_price, 0))
            else 0
        end as overage_share

  from usage_priced

  join {{ ref('dim_customer') }} dim_customer
    on dim_customer.customer_code = usage_priced.customer_code_nk

  join {{ ref('dim_product') }} dim_product
    on dim_product.product_code = usage_priced.product_code_nk

  join {{ ref('dim_plan') }} dim_plan
    on dim_plan.plan_code = usage_priced.plan_code_nk

  join {{ ref('dim_currency') }} dim_currency
    on dim_currency.currency_code = usage_priced.currency_code_nk
)

select
    customer_key
  , product_key
  , plan_key
  , currency_key
  , report_date
  , units_used
  , included_units
  , overage_units
  , unit_price
  , billed_value
  , included_value
  , overage_value
  , overage_share
from usage_enriched