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
        , load_timestamp_column = 'load_ts_utc'
    ) }}
)

select
      customer_hkey
    , customer_code
    , customer_name
    , record_source
    , load_ts_utc
from current_from_history
