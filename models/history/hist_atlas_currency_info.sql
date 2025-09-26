/**
 * hist_atlas_currency_info.sql
 * ----------------------------
 * History table for supported currencies.
 *
 * Purpose:
 * - Provide stable surrogate key for currency (currency_hkey).
 * - Track changes to names/precision with currency_hdiff.
 */

{{ save_history(
      staging_relation      = ref('stg_atlas_currency_info')
    , surrogate_key_column  = 'currency_hkey'
    , version_hash_column   = 'currency_hdiff'
) }}
