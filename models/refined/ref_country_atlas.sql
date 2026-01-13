/**
 * ref_country_atlas.sql
 * --------------------------
 * Refined dimension for countries.
 *
 * Purpose:
 * - Collapse SCD history to the current country record per surrogate key.
 * - Provide stable business attributes for joins in marts.
 *
 * Grain:
 * - One row per country_hkey.
 */

select
      country_hkey as country_key
    , country_code
    , country_name
    , record_source
    , load_ts_utc
from {{ ref('stg_atlas_country_info') }}
