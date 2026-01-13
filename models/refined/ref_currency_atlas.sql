/**
 * ref_currency_atlas.sql
 * ---------------------------
 * Refined dimension for currencies.
 *
 * Purpose:
 * - Collapse SCD history to the current currency record per surrogate key.
 * - Provide stable codes, names, and precision for joins in marts.
 *
 * Grain:
 * - One row per currency_hkey.
 */

select
      currency_hkey as currency_key
    , currency_code
    , currency_name
    , decimal_digits
    , record_source
    , load_ts_utc
from {{ ref('stg_atlas_currency_info') }}
