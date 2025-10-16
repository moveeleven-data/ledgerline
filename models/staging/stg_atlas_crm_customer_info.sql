/**
 * stg_atlas_crm_customer_info.sql
 * -------------------------------
 * Staging model for CRM customer reference data.
 *
 * Purpose:
 * - Normalize customer and country codes.
 * - Add a default row for safe joins.
 * - Generate surrogate keys for uniqueness and change tracking.
 */

with

customer_source as (
    select
          upper(customer_code)           as customer_code
        , customer_name
        , upper(country_code)            as country_code2
        , to_timestamp_ntz(load_ts)      as ingestion_ts
        , 'SEED.atlas_crm_customer_info' as record_source
    from {{ ref('atlas_crm_customer_info') }}
)

, customer_default_row as (
    select
          '-1'                           as customer_code
        , 'Missing'                      as customer_name
        , '-1'                           as country_code2
        , to_timestamp_ntz('2020-01-01') as ingestion_ts
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

, customer_hashed as (
    select
          {{ dbt_utils.generate_surrogate_key([
               'customer_code'
          ]) }} as customer_hkey

        , {{ dbt_utils.generate_surrogate_key([
               'customer_code'
              ,'customer_name'
              ,'country_code2'
          ]) }} as customer_hdiff

        , *
        , to_timestamp_ntz('{{ run_started_at }}') as pipeline_ts
    from customer_combined
)

select
    *
from customer_hashed
