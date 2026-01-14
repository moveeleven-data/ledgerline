/**
 * stg_atlas_country_info.sql
 * --------------------------
 * Staging model for country reference data.
 *
 * Purpose:
 * - Normalize codes to uppercase.
 * - Add a default row for safe joins.
 * - Generate surrogate keys for uniqueness and change tracking.
 * - Keep only the latest version per country_code by load_ts_utc.
 */

with

country_source as (
    select
          upper(country_code)            as country_code
        , country_name
        , to_timestamp_ntz(load_ts)      as load_ts_utc
        , 'SEED.atlas_country_info'      as record_source
    from {{ ref('atlas_country_info') }}
)

, country_latest as (
    select
        *
    from country_source

    qualify row_number() over (
        partition by
            country_code
        order by
            load_ts_utc desc
    ) = 1
)

, country_hashed as (
    select
          {{ dbt_utils.generate_surrogate_key(['country_code']) }} as country_hkey
        , *
    from country_latest
)

select
    *
from country_hashed
