/**
 * stg_atlas_crm_customer_info.sql
 * -------------------------------
 * Staging model for CRM customer reference data.
 *
 * Purpose:
 * - Normalize customer and country codes.
 * - Add a default row for safe joins.
 * - Generate surrogate keys for uniqueness and change tracking.
 * - Keep only the latest version per customer_code by load_ts_utc.
 */

with

customer_source as (
    select
          upper(customer_code)           as customer_code
        , customer_name
        , upper(country_code)            as country_code
        , to_timestamp_ntz(load_ts)      as load_ts_utc
        , 'SEED.atlas_crm_customer_info' as record_source
    from {{ ref('atlas_crm_customer_info') }}
)

, customer_default_row as (
    select
          '-1'                           as customer_code
        , 'Missing'                      as customer_name
        , '-1'                           as country_code
        , to_timestamp_ntz('2020-01-01') as load_ts_utc
        , 'System.DefaultKey'            as record_source
)

, customer_combined as (
    select
        *
    from customer_source

    union all

    select
        *
    from customer_default_row
)

, customer_latest as (
    select
        *
    from customer_combined

    qualify row_number() over (
        partition by
            customer_code
        order by
            load_ts_utc desc
    ) = 1
)

, customer_hashed as (
    select
          {{ dbt_utils.generate_surrogate_key(['customer_code']) }} as customer_hkey
        , {{ dbt_utils.generate_surrogate_key([
                'customer_code'
              , 'customer_name'
              , 'country_code'
            ]) }} as customer_hdiff
        
        , *
    from customer_latest
)

select
    *
from customer_hashed
