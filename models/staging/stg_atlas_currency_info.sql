/**
 * stg_atlas_currency_info.sql
 * ---------------------------
 * Staging model for currency reference data.
 *
 * Purpose:
 * - Normalize codes to uppercase.
 * - Add a default row for safe joins.
 * - Generate surrogate keys for uniqueness and change tracking.
 * - Keep only the latest version per currency_code by load_ts_utc.
 */

with

currency_source as (
    select
          upper(currency_code)        as currency_code
        , currency_name
        , decimal_digits
        , to_timestamp_ntz(load_ts)   as load_ts_utc
        , 'SEED.atlas_currency_info'  as record_source
    from {{ ref('atlas_currency_info') }}
)

, currency_default_row as (
    select
          '-1'                           as currency_code
        , 'Missing'                      as currency_name
        , 2                              as decimal_digits
        , to_timestamp_ntz('2020-01-01') as load_ts_utc
        , 'System.DefaultKey'            as record_source
)

, currency_combined as (
    select
        *
    from currency_source

    union all

    select
        *
    from currency_default_row
)

, currency_latest as (
    select
        *
    from currency_combined

    qualify row_number() over (
        partition by
            currency_code
        order by
            load_ts_utc desc
    ) = 1
)

, currency_hashed as (
    select
          {{ dbt_utils.generate_surrogate_key(['currency_code']) }} as currency_hkey
        , {{ dbt_utils.generate_surrogate_key([
                'currency_code'
              , 'currency_name'
              , 'decimal_digits'
           ]) }} as currency_hdiff
        
        , *
    from currency_latest
)

select
    *
from currency_hashed