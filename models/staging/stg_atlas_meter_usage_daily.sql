/**
 * stg_atlas_meter_usage_daily.sql
 * Staging model for daily metered usage feed.
 *
 * Purpose:
 * - Normalize codes and dates.
 * - Remove ghost rows (fully empty records).
 * - Deduplicate at the natural grain (cust x prod x plan x date).
 * - Add a default row for safe joins.
 * - Generate surrogate keys for uniqueness and change tracking.
 */

with

source_usage as (
    select
          upper(customer_code)                       as customer_code
        , upper(product_code)                        as product_code
        , upper(plan_code)                           as plan_code
        , {{ to_21st_century_date('report_date') }}  as report_date
        , try_to_number(units_used)                  as units_used
        , try_to_number(included_units)              as included_units
        , to_timestamp_ntz(load_ts)                  as load_ts_utc
        , 'atlas_meter'                              as record_source
    from {{ source('atlas_meter', 'atlas_meter_usage_daily') }}
)

, ghost_rows_removed as (
    select *
    from source_usage
    where customer_code   is not null
      and product_code    is not null
      and plan_code       is not null
      and report_date     is not null
      and units_used      is not null
      and included_units  is not null
)

, deduplicated_usage as (
    select
        *
    from ghost_rows_removed

    qualify row_number() over (
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

, default_row as (
    select
          '-1'                           as customer_code
        , '-1'                           as product_code
        , '-1'                           as plan_code
        , to_date('2020-01-01')          as report_date
        , 0::number                      as units_used
        , 0::number                      as included_units
        , to_timestamp_ntz('2020-01-01') as load_ts_utc
        , 'System.DefaultKey'            as record_source
)

, combined_usage as (
    select
        *
    from deduplicated_usage

    union all

    select
        *
    from default_row
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

        , {{ dbt_utils.generate_surrogate_key([
               'customer_code'
             , 'product_code'
             , 'plan_code'
             , "to_varchar(report_date,'YYYY-MM-DD')"
             , 'units_used'
             , 'included_units'
          ]) }} as usage_hdiff

        , *
    from combined_usage
)

select
    *
from hashed_usage
