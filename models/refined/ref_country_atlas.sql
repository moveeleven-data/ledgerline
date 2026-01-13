/**
 * ref_country_atlas.sql
 * --------------------------
 * Country dimension for the star schema.
 *
 * Purpose:
 * - Surface country codes and names directly from staging to support geography joins.
 * - Alias `country_hkey` to `country_key` to match the star schema convention.
 *
 * Grain:
 * - One row per country_key (from staging).
 */

select
      country_hkey as country_key
    , country_code
    , country_name
    , record_source
    , load_ts_utc
from {{ ref('stg_atlas_country_info') }}
