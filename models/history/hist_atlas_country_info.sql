/**
 * hist_atlas_country_info.sql
 * ---------------------------
 * History table for country reference data.
 *
 * Purpose:
 * - Maintain stable country dimension across time.
 * - Use country_hkey as surrogate ID.
 * - Track attribute changes with country_hdiff.
 */

{{ save_history(
      staging_relation      = ref('stg_atlas_country_info')
    , surrogate_key_column  = 'country_hkey'
    , version_hash_column   = 'country_hdiff'
) }}
