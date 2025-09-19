select
    account_key
    , security_key
    , exchange_key
    , currency_key
    , report_date
    , count(*) as row_count
from {{ ref('FACT_POSITION') }}
group by 1, 2, 3, 4, 5
order by row_count desc