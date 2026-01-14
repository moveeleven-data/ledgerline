/**
 * dim_country.sql
 * ---------------
 * Pass-through of the staging country reference.
 * Grain: one row per country_key.
 */

select
      country_hkey as country_key
    , country_code
    , country_name
from {{ ref('stg_atlas_country_info') }}
