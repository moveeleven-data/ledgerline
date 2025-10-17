{{ config(
     materialized = 'table'
   , tags = ['mart:usage', 'intermediate', 'domain:usage_billing', 'eda']
) }}

/**
 * int_fact_usage_priced_window.sql
 * --------------------------------
 * Priced usage for a user-defined date window (EDA only).
 */

{% set eda_start_date = env_var('DBT_EDA_START_DATE', '1900-01-01') %}
{% set eda_end_date   = env_var('DBT_EDA_END_DATE',   '2100-01-01') %}

with usage_window as (
    select
          report_date
        , customer_code
        , product_code
        , plan_code
        , units_used
        , included_units
        , greatest(units_used - included_units, 0) as overage_units
    from {{ ref('ref_usage_atlas') }}
    where report_date between to_date('{{ eda_start_date }}') and to_date('{{ eda_end_date }}')
)

, price_book as (
    select
          product_code
        , plan_code
        , price_date
        , unit_price
        , '{{ var("default_billing_currency","USD") }}'::string as currency_code
    from {{ ref('stg_atlas_price_book_daily') }}
)

, priced as (
    select
          usage_window_data.report_date
        , usage_window_data.customer_code    as customer_code_nk
        , usage_window_data.product_code     as product_code_nk
        , usage_window_data.plan_code        as plan_code_nk
        , usage_window_data.units_used
        , usage_window_data.included_units
        , usage_window_data.overage_units
        , price_book_data.unit_price
        , coalesce(price_book_data.currency_code, '{{ var("default_billing_currency","USD") }}') as currency_code_nk
    from usage_window as usage_window_data
    left join price_book as price_book_data
           on price_book_data.product_code = usage_window_data.product_code
          and price_book_data.plan_code    = usage_window_data.plan_code
          and price_book_data.price_date  <= usage_window_data.report_date
    qualify row_number() over (
                partition by
                      usage_window_data.report_date
                    , usage_window_data.product_code
                    , usage_window_data.plan_code
                order by price_book_data.price_date desc nulls last
            ) = 1
)

select
      report_date
    , customer_code_nk
    , product_code_nk
    , plan_code_nk
    , units_used
    , included_units
    , overage_units
    , unit_price
    , currency_code_nk
from priced
