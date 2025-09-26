/**
 * hist_atlas_catalog_plan_info.sql
 * --------------------------------
 * History table for subscription plans.
 *
 * Purpose:
 * - Track changes to plan attributes over time (SCD2).
 * - Generate OPEN and CLOSE rows from the staging feed.
 * - Use plan_hkey as the stable surrogate identifier.
 * - Use plan_hdiff to detect attribute changes.
 */

{{ save_history(
      staging_relation      = ref('stg_atlas_catalog_plan_info')
    , surrogate_key_column  = 'plan_hkey'
    , version_hash_column   = 'plan_hdiff'
) }}
