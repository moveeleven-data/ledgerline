/**
 * dim_currency.sql
 * ----------------
 * Pass-through of the refined currency dimension.
 * Grain: one row per currency_key.
 */

select
      currency_hkey as currency_key
    , currency_code
    , cast(
         decimal_digits as number(38,0)
      ) as decimal_digits
    , currency_name
from {{ ref('ref_currency_atlas') }}
