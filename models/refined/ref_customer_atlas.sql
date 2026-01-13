/**
 * ref_customer_atlas.sql
 * -------------------------------
 * Customer dimension for the star schema.
 *
 * Purpose:
 * - Serve as a thin wrapper around the staging table, exposing stable customer attributes (code, name, country) for marts.
 * - Rename `customer_hkey` to `customer_key` to standardize surrogate key naming across the project.
 *
 * Grain:
 * - One row per customer_key (from staging).
 */

select
      customer_hkey as customer_key
    , customer_code
    , customer_name
    , country_code
    , record_source
    , load_ts_utc
from {{ ref('stg_atlas_crm_customer_info') }}
