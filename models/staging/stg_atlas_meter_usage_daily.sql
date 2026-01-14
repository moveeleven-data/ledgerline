/**
 * stg_atlas_meter_usage_daily.sql
 * Staging model for daily metered usage feed.
 *
 * Purpose:
 * - Normalize codes and dates.
 * - Enforce numeric precision for usage fields.
 * - Deduplicate at the natural grain (cust x prod x plan x date).
 * - Generate surrogate keys for uniqueness.
 */

with

source_usage as (
    select
          upper(customer_code)                            as customer_code
        , upper(product_code)                             as product_code
        , upper(plan_code)                                as plan_code
        , {{ to_21st_century_date('report_date') }}::date as report_date

        , try_to_number(units_used)::number(38,0)         as units_used
        , try_to_number(included_units)::number(38,0)     as included_units

        , to_timestamp_ntz(load_ts)                       as load_ts_utc
        , 'SEED.atlas_meter_usage_daily' as record_source
    from {{ ref('atlas_meter_usage_daily') }}
)

, deduplicated_usage as (
    select
        *
    from source_usage

    qualify
        row_number() over (
            partition by
                customer_code
              , product_code
              , plan_code
              , report_date
            order by
                load_ts_utc desc
              , units_used desc
        ) = 1
)

, hashed_usage as (
    select
          {{ dbt_utils.generate_surrogate_key(['customer_code']) }} as customer_hkey
        , {{ dbt_utils.generate_surrogate_key(['product_code'])  }} as product_hkey
        , {{ dbt_utils.generate_surrogate_key(['plan_code'])     }} as plan_hkey

        , {{ dbt_utils.generate_surrogate_key([
               'customer_code'
             , 'product_code'
             , 'plan_code'
             , 'report_date'
          ]) }} as usage_hkey

        , *
    from deduplicated_usage
)

select
    *
from hashed_usage
