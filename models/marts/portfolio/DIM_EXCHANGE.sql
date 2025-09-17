select
    exchange_hkey   as exchange_key
  , exchange_code   as exchange_code
  , exchange_name   as exchange_name
  , country_name    as country_name
  , city_name       as city_name
  , timezone_name   as timezone_name
  , tz_delta_hours  as tz_delta_hours
  , dst_period_desc as dst_period_desc
  , open_local      as open_local
  , close_local     as close_local
  , lunch_local     as lunch_local
  , open_utc        as open_utc
  , close_utc       as close_utc
  , lunch_utc       as lunch_utc
from {{ ref('REF_EXCHANGE_ABC_BANK') }}
where exchange_code <> '-1'