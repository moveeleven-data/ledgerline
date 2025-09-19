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
    'ACC001'           -- known account
  , 'ZZ_TEST_SEC'      -- not in ref_security_abc_bank
  , 'AMS'              -- known exchange from exchange seed
  , to_date('2024-07-15')
  , 10
  , 100
  , 120
  , 'AED'              -- known currency from currency seed
  , current_timestamp()
)