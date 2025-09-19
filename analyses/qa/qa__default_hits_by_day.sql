{{ config(tags=['qa']) }}

-- QA Check: Foreign key coverage and default-member usage for FACT_POSITION

-- Inputs:
--   as_of_date: target reporting date (YYYY-MM-DD, defaults to run_started_at)

{% set as_of_date = var('as_of_date', run_started_at.strftime('%Y-%m-%d')) %}

select
    f.report_date
  , count_if(a.account_key  is null)                                as missing_account_dim
  , count_if(s.security_key is null)                                as missing_security_dim
  , count_if(x.exchange_key is null)                                as missing_exchange_dim
  , count_if(c.currency_key is null)                                as missing_currency_dim
  , count_if(s.security_key is not null and s.security_code = '-1') as hits_security_default

from {{ ref('FACT_POSITION') }} f
left join {{ ref('DIM_ACCOUNT')  }}  a on a.account_key  = f.account_key
left join {{ ref('DIM_SECURITY') }}  s on s.security_key = f.security_key
left join {{ ref('DIM_EXCHANGE') }}  x on x.exchange_key = f.exchange_key
left join {{ ref('DIM_CURRENCY') }}  c on c.currency_key = f.currency_key

where f.report_date = to_date('{{ as_of_date }}')
group by f.report_date
