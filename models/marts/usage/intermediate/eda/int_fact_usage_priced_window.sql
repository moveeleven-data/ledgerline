{{ config(
     materialized = 'table'
   , tags = ['mart:usage', 'intermediate', 'domain:usage_billing', 'eda']
) }}

/**
 * int_fact_usage_priced_window.sql
 * ---------------------------------
 * Prepare priced daily usage for a given date window at the grain:
 *   customer_code × product_code × plan_code × report_date.
 *
 * What this does:
 * - Reads normalized daily usage for a configurable date window.
 * - Calculates overage_units.
 * - Finds the last effective unit_price on or before report_date.
 * - Assigns a consistent currency_code during pricing.
 *
 * Notes:
 * - The window is controlled by env vars DBT_EDA_START_DATE and DBT_EDA_END_DATE.
 * - Filters out the synthetic default row.
 */

{% set eda_start_date = env_var('DBT_EDA_START_DATE', '1900-01-01') %}
{% set eda_end_date   = env_var('DBT_EDA_END_DATE',   '2100-01-01') %}

with

/* Step 1. Prepare usage records for the date window.
   - Keep only rows within the configured window.
   - Standardize numeric fields and compute overage_units.
   - Replace missing keys with '-1' as a placeholder.
   - Drop the synthetic default row. */

normalized_usage as (
    select
          usage.report_date::date                              as report_date
        , cast(usage.units_used as number(38,0))               as units_used
        , cast(usage.included_units as number(38,0))           as included_units
        , greatest(usage.units_used - usage.included_units, 0) as overage_units
        , coalesce(usage.customer_code, '-1')                  as customer_code_nk
        , coalesce(usage.product_code,  '-1')                  as product_code_nk
        , coalesce(usage.plan_code,     '-1')                  as plan_code_nk
    from {{ ref('stg_atlas_meter_usage_daily') }} as usage
    where
        usage.report_date between
            to_date('{{ eda_start_date }}') and to_date('{{ eda_end_date }}')
      and not (usage.customer_code = '-1' and usage.product_code = '-1' and usage.plan_code = '-1')
)

/* Step 2. Bring in the daily price book.
   - Include unit prices for each product–plan.
   - Add a standard currency code (default is USD). */

, price_book as (
    select
          price.product_code
        , price.plan_code
        , price.price_date
        , price.unit_price
        , '{{ var("default_billing_currency","USD") }}'::string as currency_code
    from {{ ref('stg_atlas_price_book_daily') }} as price
)

/* Step 3. Match each usage record with the right price.
   - Join to the price book using prices effective on or before the usage date.
   - Pick the most recent valid price for each day. */

, usage_priced as (
    select
          usage.report_date
        , usage.customer_code_nk
        , usage.product_code_nk
        , usage.plan_code_nk
        , usage.units_used
        , usage.included_units
        , usage.overage_units
        , price.unit_price
        , coalesce(
              price.currency_code
            , '{{ var("default_billing_currency","USD") }}'
          ) as currency_code_nk
    from normalized_usage as usage
    left join price_book as price
           on price.product_code = usage.product_code_nk
          and price.plan_code    = usage.plan_code_nk
          and price.price_date  <= usage.report_date

    qualify
        row_number() over (
            partition by
                 usage.report_date
               , usage.customer_code_nk
               , usage.product_code_nk
               , usage.plan_code_nk
            order by
                 price.price_date desc nulls last
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
from usage_priced
