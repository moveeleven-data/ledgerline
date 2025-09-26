/**
 * hist_atlas_crm_customer_info.sql
 * --------------------------------
 * History table for CRM customer attributes.
 *
 * Purpose:
 * - Track customer identity and geography over time.
 * - Ensure one surrogate per natural key (customer_hkey).
 * - Detect changes with customer_hdiff.
 */

{{ save_history(
      staging_relation      = ref('stg_atlas_crm_customer_info')
    , surrogate_key_column  = 'customer_hkey'
    , version_hash_column   = 'customer_hdiff'
) }}
