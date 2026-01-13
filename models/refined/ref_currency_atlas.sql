/**
 * ref_currency_atlas.sql
 * ---------------------------
 * Currency dimension for the star schema.
 *
 * Purpose:
 * - Provide a stable set of currency attributes (code, name, decimal_digits) for marts and facts.
 * - Rename `currency_hkey` to `currency_key` to standardize key naming.
 *
 * Grain:
 * - One row per currency_key (from staging).
 */

select
      currency_hkey as currency_key
    , currency_code
    , currency_name
    , decimal_digits
    , record_source
    , load_ts_utc
from {{ ref('stg_atlas_currency_info') }}
