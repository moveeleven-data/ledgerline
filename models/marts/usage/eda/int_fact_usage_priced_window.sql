{{ config(materialized = 'table') }}

-- int_fact_usage_priced_window.sql
-- EDA pricing over a date window; metrics included.

{% set eda_start_date = env_var('DBT_EDA_START_DATE', '1900-01-01') %}
{% set eda_end_date   = env_var('DBT_EDA_END_DATE',   '2100-01-01') %}

with

usage_window as (
    select
          report_date
        , units_used
        , included_units
        , greatest(units_used - included_units, 0) as overage_units
        , customer_hkey as customer_key
        , product_hkey as product_key
        , plan_hkey as plan_key
        , product_code
        , plan_code
    from {{ ref('ref_usage_atlas') }}
    where
        report_date between to_date('{{ eda_start_date }}') and to_date('{{ eda_end_date }}')
)

, priced as (
    select
          usage_records.report_date
        , usage_records.customer_key
        , usage_records.product_key
        , usage_records.plan_key
        , usage_records.units_used
        , usage_records.included_units
        , usage_records.overage_units
        , coalesce(price_book.unit_price, 0) as unit_price
        , price_book.currency_key as currency_key

    from usage_window as usage_records
    left join {{ ref('ref_price_book_daily') }} as price_book
           on price_book.product_code = usage_records.product_code
          and price_book.plan_code    = usage_records.plan_code
          and price_book.price_date  <= usage_records.report_date

    qualify row_number() over (
        partition by
            usage_records.report_date
          , usage_records.product_code
          , usage_records.plan_code
        order by
            price_book.price_date desc nulls last
    ) = 1
)

, metrics as (
    select
          priced.*
        , (units_used     * unit_price) as billed_value
        , (included_units * unit_price) as included_value
        , (overage_units  * unit_price) as overage_value
        , case when (units_used * unit_price) > 0
            then (overage_units * unit_price)
                 / (units_used * unit_price)
            else 0
          end as overage_share
    from priced
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
    , billed_value
    , included_value
    , overage_value
    , overage_share
from metrics
