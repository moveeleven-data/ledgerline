/**
 * dim_country.sql
 * ---------------
 * Pass-through of the refined country dimension.
 * Grain: one row per country_key.
 */

select
      country_hkey as country_key
    , country_name
    , country_code
from {{ ref('ref_country_atlas') }}
