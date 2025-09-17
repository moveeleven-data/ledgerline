{{ config(materialized='ephemeral') }}

with

src_positions as (
    select
        upper(accountid)                            as account_code
        , upper(symbol)                             as security_code
        , upper(exchange)                           as exchange_code
        , {{ to_21st_century_date('report_date') }} as report_date
        , quantity                                  as quantity
        , cost_base                                 as cost_base
        , position_value                            as position_value
        , upper(currency)                           as currency_code
        , ingested_at                               as source_loaded_at_utc
        , 'SOURCE_DATA.abc_bank_position'           as record_source
    from {{ source('abc_bank', 'abc_bank_position') }}
)

, security_lookup as (
    select
        upper(security_code) as security_code
        , security_name
    from {{ ref('abc_bank_security_info') }}
)

, enriched_positions as (
    select
        pos.account_code
      , pos.security_code
      , coalesce(sec.security_name, 'Missing') as security_name
      , pos.exchange_code
      , pos.report_date
      , pos.quantity
      , pos.cost_base
      , pos.position_value
      , pos.currency_code
      , pos.source_loaded_at_utc
      , pos.record_source
    from src_positions as pos
    left join security_lookup as sec
      on pos.security_code = sec.security_code
)

, deduped_positions as (
  select *
  from enriched_positions
  qualify row_number() over (
           -- one open position per (account, security, normalized date)
           partition by
               account_code,
               security_code,
               report_date
           order by
               source_loaded_at_utc desc,
               position_value desc
         ) = 1
)

, hashed_positions as (
    select
        {{ dbt_utils.generate_surrogate_key(['account_code','security_code']) }} as position_hkey,
        {{ dbt_utils.generate_surrogate_key([
            'account_code',
            'security_code',
            'security_name',
            'exchange_code',
            "to_varchar(report_date, 'YYYY-MM-DD')",
            'quantity',
            'cost_base',
            'position_value',
            'currency_code'
        ]) }} as position_hdiff

        , *
        , to_timestamp_ntz('{{ run_started_at }}') as load_ts_utc
    from deduped_positions
)

select * from hashed_positions