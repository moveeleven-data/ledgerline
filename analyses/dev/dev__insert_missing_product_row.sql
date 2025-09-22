{{ config(tags = ['dev']) }}

/*
   Dev Helper: insert a FACT_USAGE row with a missing product_code
   Goal: validate DIM_PRODUCT self-completion behavior when fact references unknown product
   Notes:
     - customer_code and plan_code map to known seeds
     - product_code 'ZZ_TEST_PROD' does not exist in ref_product_atlas
     - row dated 2024-07-15 with simple units
*/

insert into {{ ref('usage_daily') }} (
    customer_code
  , product_code
  , plan_code
  , report_date
  , units_used
  , included_units
  , load_ts
)
values (
    'CUST001'        -- valid customer
  , 'ZZ_TEST_PROD'   -- deliberately unknown product
  , 'PLAN_BASIC'     -- valid plan
  , to_date('2024-07-15')
  , 10
  , 5
  , current_timestamp()
);
