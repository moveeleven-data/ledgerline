{{ config(
     materialized = 'table'
   , tags = [
       'mart:usage'
     , 'intermediate'
     , 'domain:usage_billing'
     , 'eda']
) }}

/**
 * int_fact_usage_priced_window.sql
 * --------------------------------
 * EDA pricing over a date window. Keys flow from REF usage.
 */

{% set eda_start_date = env_var('DBT_EDA_START_DATE', '1900-01-01') %}
{% set eda_end_date   = env_var('DBT_EDA_END_DATE',   '2100-01-01') %}

with usage_window as (
    select
          report_date
        , units_used
        , included_units
        , greatest(units_used - included_units, 0) as overage_units
        , customer_hkey                             as customer_key
        , product_hkey                              as product_key
        , plan_hkey                                 as plan_key
        , product_code
        , plan_code
    from {{ ref('ref_usage_atlas') }}
    where
        report_date between to_date('{{ eda_start_date }}') and to_date('{{ eda_end_date }}')
)

, price_book as (
    select
          product_code
        , plan_code
        , price_date
        , unit_price
        , {{ var('default_billing_currency', 'USD') }}::string as currency_code_nk
    from {{ ref('ref_price_book_daily') }}
)

, priced as (
    select
          u.report_date
        , u.customer_key
        , u.product_key
        , u.plan_key
        , u.units_used
        , u.included_units
        , u.overage_units
        , p.unit_price
        , p.currency_code_nk as currency_code_nk
        , {{ dbt_utils.generate_surrogate_key(['currency_code_nk']) }} as currency_key

    from usage_window as u
    left join price_book as p
           on p.product_code = u.product_code
          and p.plan_code    = u.plan_code
          and p.price_date  <= u.report_date

    qualify row_number() over (
        partition by
            u.report_date
          , u.product_code, u.plan_code
        order by
            p.price_date desc nulls last
    ) = 1
)

select
      report_date
    , customer_key
    , product_key
    , plan_key
    , currency_key
    , units_used
    , included_units
    , overage_units
    , unit_price
from priced
