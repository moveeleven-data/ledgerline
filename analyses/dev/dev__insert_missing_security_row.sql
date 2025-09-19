{{ config(tags=['dev']) }}

/*
   Dev Helper: insert a FACT_POSITION row with a missing security_code
   Goal: validate DIM_SECURITY self-completion behavior when fact references unknown security
   Notes:
     - accountid, exchange, and currency all map to known seeds
     - symbol 'ZZ_TEST_SEC' does not exist in ref_security_abc_bank
     - row dated 2024-07-15, with small sample values for quantity, cost, and value
*/

insert into {{ source('abc_bank','abc_bank_position') }} (
    accountid
  , symbol
  , exchange
  , report_date
  , quantity
  , cost_base
  , position_value
  , currency
  , ingested_at
)
values (
    'ACC001'           -- valid account
  , 'ZZ_TEST_SEC'      -- deliberately unknown security
  , 'AMS'              -- valid exchange
  , to_date('2024-07-15')
  , 10
  , 100
  , 120
  , 'AED'              -- valid currency
  , current_timestamp()
)