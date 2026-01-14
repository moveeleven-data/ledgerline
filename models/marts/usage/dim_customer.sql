/**
 * dim_customer.sql
 * ----------------
 * Pass-through of the staging customer reference.
 * Grain: one row per customer_key.
 */

select
      customer_hkey as customer_key
    , customer_code
    , customer_name
    , country_code
from {{ ref('stg_atlas_crm_customer_info') }}
