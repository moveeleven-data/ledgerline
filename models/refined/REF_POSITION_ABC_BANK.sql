{{ config(materialized='table') }}

with latest_any as (
  select *
  from {{ ref('HIST_ABC_BANK_POSITION') }}
  qualify row_number() over (
    partition by position_hkey
    order by report_date desc, load_ts_utc desc
  ) = 1
),

latest_open as (
  select *
  from latest_any
  where position_row_type = 'OPEN'
    and quantity       is not null
    and cost_base      is not null
    and position_value is not null
    and currency_code  is not null
    and cost_base <> 0
)

select
    position_hkey
  , account_code
  , security_code
  , security_name
  , exchange_code
  , currency_code
  , record_source
  , report_date
  , quantity
  , cost_base
  , position_value
  , load_ts_utc
  , position_row_type

  , cast(position_value - cost_base as number(38,2)) as unrealized_profit
  , cast(
        round(
            (position_value - cost_base) / nullif(cost_base, 0) * 100
           , 5
        ) as number(38,5)
    ) as unrealized_profit_pct

from latest_open