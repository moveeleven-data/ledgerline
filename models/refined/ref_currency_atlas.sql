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

with

current_from_history as (
    {{ current_from_history(
          history_relation      = ref('hist_atlas_currency_info')
        , key_column            = 'currency_hkey'
        , load_timestamp_column = 'load_ts_utc'
    ) }}
)

select
      currency_hkey
    , currency_code
    , currency_name
    , decimal_digits
    , record_source
    , load_ts_utc
from current_from_history
