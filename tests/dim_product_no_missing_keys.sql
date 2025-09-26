/**
 * dim_product_no_missing_keys.sql
 * --------------------------------
 * Warns if product_codes appear in the usage fact table but are missing
 * from the product dimension (dim_product).
 *
 * Purpose:
 * Ensures referential integrity between facts and dimensions.
 * Every usage row must map to a valid product in dim_product.
 *
 * Behavior:
 * - fact_codes: Collects distinct product_codes from ref_usage_atlas (the usage fact source).
 * - dim_codes: Collects product_codes present in dim_product.
 * - Final select Returns any fact_codes not found in dim_codes.
 */

{{ config(severity = 'warn') }}

with

usage_products as (
    select distinct
        product_code
    from {{ ref('ref_usage_atlas') }}
    where
        product_code is not null
)

, dimension_products as (
    select
        product_code
    from {{ ref('dim_product') }}
)

, orphaned_products as (
    select
        usage_products.product_code
    from usage_products
    left join dimension_products
        using (product_code)
    where
        dimension_products.product_code is null
)

select
    *
from orphaned_products