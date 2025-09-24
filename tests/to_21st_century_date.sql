/**
 * to_21st_century_date test
 * -------------------------
 * Validates that the macro `to_21st_century_date` correctly normalizes
 * edge-case source dates into expected 21st-century dates.
 */

with test_data as (
    select
          '0021-09-23'::date as src_date
        , '2021-09-23'::date as expected_date
    union all
    select
          '1021-09-24'::date
        , '1021-09-24'::date
    union all
    select
          '2021-09-25'::date
        , '2021-09-25'::date
    union all
    select
          '-0021-09-26'::date
        , '1979-09-26'::date
)

select
      src_date
    , expected_date
    , {{ to_21st_century_date('src_date') }} as actual_date
from test_data
where {{ to_21st_century_date('src_date') }} <> expected_date