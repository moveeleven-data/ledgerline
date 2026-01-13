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

select
      customer_hkey as customer_key
    , customer_code
    , customer_name
    , country_code
    , record_source
    , load_ts_utc
from {{ ref('stg_atlas_crm_customer_info') }}
