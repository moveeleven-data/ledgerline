{{ config(materialized='ephemeral') }}

/**
 * stg_atlas_currency_info.sql
 * ---------------------------
 * Staging model for currency reference data.
 *
 * Purpose:
 * - Normalize codes to uppercase.
 * - Add a default row for safe joins.
 * - Generate surrogate keys for uniqueness and change tracking.
 */

with

currency_source as (
    select
          upper(currency_code)           as currency_code
        , currency_name
        , decimal_digits
        , to_timestamp_ntz(load_ts)      as load_ts
        , 'SEED.atlas_ref_currency_info' as record_source
    from {{ ref('atlas_currency_info') }}
)

, currency_default_row as (
    select
          '-1'                           as currency_code
        , 'Missing'                      as currency_name
        , 2                              as decimal_digits
        , to_timestamp_ntz('2020-01-01') as load_ts
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

, currency_hashed as (
    select
          {{ dbt_utils.generate_surrogate_key(['currency_code']) }} as currency_hkey
        , {{ dbt_utils.generate_surrogate_key([
               'currency_code'
              ,'currency_name'
              ,'decimal_digits'
          ]) }} as currency_hdiff

        , * exclude (load_ts)
        , to_timestamp_ntz('{{ run_started_at }}') as load_ts_utc
    from currency_combined
)

select
    *
from currency_hashed