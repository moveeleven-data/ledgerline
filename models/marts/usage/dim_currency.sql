-- dim_currency.sql

/* 
We treat currency as a closed domain and make the fact explicitly carry the billing currency.
We do not self-complete the currency dimension. Self-completion hides bad data like “USDD” or “EURO”.
Closed domains should fail fast, not auto-invent rows.

We put currency_code on the fact at pricing time and keep dim_currency a plain select from the refined table.
*/

select
    currency_hkey as currency_key
  , currency_code
  , cast(decimal_digits as number(38,0)) as decimal_digits
  , currency_name
from {{ ref('ref_currency_atlas') }}
where currency_code is not null
