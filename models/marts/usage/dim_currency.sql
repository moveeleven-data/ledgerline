/**
 * dim_currency.sql
 * ----------------
 * Currency is a closed domain. The price book and all billing math assume
 * that codes come from a vetted reference list. We do not self-complete this
 * dimension, because fabricating rows would hide bad data (for example USDD).
 *
 * Design rules:
 * - Fact rows must carry an explicit currency_code at pricing time.
 * - This dimension is a plain projection from the refined reference table.
 * - Tests in usage.yml enforce uniqueness and relationships.
 */

-- Step 1. Select the authoritative rows from the refined layer.
--         Keep only valid codes to fail fast when upstream inputs are wrong.

select
    currency_hkey as currency_key
  , currency_code
  , cast(decimal_digits as number(38,0)) as decimal_digits
  , currency_name
from {{ ref('ref_currency_atlas') }}
where 
    currency_code is not null