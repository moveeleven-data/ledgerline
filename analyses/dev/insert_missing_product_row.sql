/**
 * dev__insert_missing_product_row.sql
 * -----------------------------------
 * Dev helper query.
 *
 * Purpose:
 * - Insert a single fact_usage row with a product_code that does not exist in DIM_PRODUCT.
 * - This simulates a data quality issue and validates that the
 *   self_completing_dimension macro fills in a default row.
 *
 * Setup:
 * - customer_code and plan_code map to known valid seeds.
 * - product_code 'ZZ_TEST_PROD' is deliberately missing from ref_product_atlas.
 * - Row is dated 2024-07-15 with small unit values.
 */

insert into {{ ref('fact_usage') }} (
    customer_code
  , product_code
  , plan_code
  , report_date
  , units_used
  , included_units
  , load_ts
)

values (
    'CUST001'              -- valid customer from seed
  , 'ZZ_TEST_PROD'         -- deliberately unknown product to trigger dimension backfill
  , 'PLAN_BASIC'           -- valid plan from seed
  , to_date('2024-07-15')  -- chosen test date
  , 10                     -- small number of units used
  , 5                      -- included units
  , current_timestamp()    -- load timestamp for lineage
);
