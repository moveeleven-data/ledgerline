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

with

current_from_history as (
    {{ current_from_history(
          history_relation      = ref('hist_atlas_country_info')
        , key_column            = 'country_hkey'
        , load_timestamp_column = 'load_ts_utc'
    ) }}
)

select
      country_hkey
    , country_code
    , country_name
    , record_source
    , load_ts_utc
from current_from_history