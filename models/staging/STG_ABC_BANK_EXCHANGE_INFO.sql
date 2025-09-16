{{ config(materialized='ephemeral') }}

with

src_exchanges as (
    select
        id                               as exchange_code
        , name                           as exchange_name
        , country                        as country_name
        , city                           as city_name
        , zone                           as timezone_name
        , delta                          as tz_delta_hours
        , dst_period                     as dst_period_desc
        , open                           as open_local
        , close                          as close_local
        , lunch                          as lunch_local
        , open_utc                       as open_utc
        , close_utc                      as close_utc
        , lunch_utc                      as lunch_utc
        , load_ts                        as load_ts
        , 'SEED.abc_bank_exchange_info'  as record_source
    from {{ ref('abc_bank_exchange_info') }}
)

, default_exchange_record as (
    select
        '-1'                              as exchange_code
        , 'Missing'                       as exchange_name
        , null                            as country_name
        , null                            as city_name
        , null                            as timezone_name
        , null                            as tz_delta_hours
        , null                            as dst_period_desc
        , null                            as open_local
        , null                            as close_local
        , null                            as lunch_local
        , null                            as open_utc
        , null                            as close_utc
        , null                            as lunch_utc
        , to_timestamp_ntz('2020-01-01')  as load_ts
        , 'System.DefaultKey'             as record_source
)

, exchanges_with_default as (
    select * from src_exchanges
    union all
    select * from default_exchange_record
)

, hashed_exchanges as (
    select
        {{ dbt_utils.generate_surrogate_key(['exchange_code']) }} as exchange_hkey
        , {{ dbt_utils.generate_surrogate_key([
            'exchange_code'
            , 'exchange_name'
            , 'country_name'
            , 'city_name'
            , 'timezone_name'
            , "coalesce(open_utc,'')"
            , "coalesce(close_utc,'')"
        ]) }} as exchange_hdiff

        , * exclude (load_ts)
        , load_ts as load_ts_utc
    from exchanges_with_default
)

select * from hashed_exchanges