/**
 * ref_customer_atlas.sql
 * -------------------------------
 * Refined dimension for customers.
 *
 * Purpose:
 * - Collapse SCD history to the current customer record per surrogate key.
 * - Provide stable attributes for geography and identity in marts.
 *
 * Grain:
 * - One row per customer_hkey.
 */

with

current_from_history as (
    {{ current_from_history(
          history_relation      = ref('hist_atlas_crm_customer_info')
        , key_column            = 'customer_hkey'
        , load_timestamp_column = 'ingestion_ts'
    ) }}
)

select
      customer_hkey
    , customer_code
    , customer_name
    , country_code2
    , record_source
    , ingestion_ts
from current_from_history
