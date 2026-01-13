/**
 * dim_customer.sql
 * ----------------
 * Pass-through of the refined customer dimension.
 * Grain: one row per customer_key.
 */

select
      customer_key
    , customer_code
    , customer_name
    , country_code
from {{ ref('ref_customer_atlas') }}