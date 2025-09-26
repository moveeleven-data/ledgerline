/**
 * stg_atlas_country_info.sql
 * --------------------------
 * Staging model for country reference data.
 *
 * Purpose:
 * - Normalize codes to uppercase.
 * - Add a default row for safe joins.
 * - Generate surrogate keys for uniqueness and change tracking.
 */

with

country_source as (
    select
          upper(country_code)           as country_code2
        , country_name
        , to_timestamp_ntz(load_ts)     as load_ts
        , 'SEED.atlas_ref_country_info' as record_source
    from {{ ref('atlas_country_info') }}
)

, country_default_row as (
    select
          '-1'                           as country_code2
        , 'Missing'                      as country_name
        , to_timestamp_ntz('2020-01-01') as load_ts
        , 'System.DefaultKey'            as record_source
)

, country_combined as (
    select
        *
    from country_source

    union all
    
    select
        *
    from country_default_row
)

, country_hashed as (
    select
          {{ dbt_utils.generate_surrogate_key(['country_code2']) }} as country_hkey
        , {{ dbt_utils.generate_surrogate_key([
               'country_code2'
              ,'country_name'
          ]) }} as country_hdiff

        , * exclude (load_ts)
        , to_timestamp_ntz('{{ run_started_at }}') as load_ts_utc
    from country_combined
)

select
    *
from country_hashed