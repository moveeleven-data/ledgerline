{{ config(tags = ['qa']) }}

/**
 * qa__dim_product_missing_keys.sql
 * --------------------------------
 * Identify product_codes present in facts but missing from the Refined Product model (ref_product_atlas). 
 * Check whether those missing codes were added later by the Mart-level dimension (dim_product) via self-completion.
 *
 * Purpose:
 * - Validate whether the self-completing dimension pattern is working as expected.
 *
 * Input:
 * - No parameters. Runs against all distinct fact product_codes.
 *
 * Output:
 * - List of fact-level Product codes, flagged as:
 *     base_presence = not_in_refined / present_in_refined
 *     dim_presence  = missing_in_dim / present_in_dim
 */


-- Distinct product codes observed in Usage facts.

with fact_codes as (
    select distinct
        product_code
    from {{ ref('ref_usage_atlas') }}
    where
        product_code is not null
)


-- Product codes currently available in the Refined Product model.

, refined_product_codes as (
    select
        product_code
    from {{ ref('ref_product_atlas') }}
)


-- Product codes currently available in the mart product dimension.

, dim_codes as (
    select
        product_code
    from {{ ref('dim_product') }}
)


select
    fact_codes.product_code

  , case
        when refined_product_codes.product_code is null then 'not_in_refined'
        else 'present_in_refined'
    end as refined_presence

  , case
        when dim_codes.product_code is null then 'missing_in_dim'
        else 'present_in_dim'
    end as dim_presence

from fact_codes                                                         -- Product codes in Usage facts.
left join refined_product_codes                                         -- Product codes in Refined Product model.
       on refined_product_codes.product_code = fact_codes.product_code
left join dim_codes                                                     -- Product codes in Product dimension.
       on dim_codes.product_code = fact_codes.product_code

where
    refined_product_codes.product_code is null
order by
    fact_codes.product_code;