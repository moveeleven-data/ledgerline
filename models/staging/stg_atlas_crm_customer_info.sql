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

, customer_latest as (
    select
        *
    from customer_source

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
        , *
    from customer_latest
)

select
    *
from customer_hashed
