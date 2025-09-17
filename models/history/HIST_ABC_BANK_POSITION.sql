{{ config(
  materialized = 'incremental'
  , incremental_strategy = 'merge'
  , unique_key = ['POSITION_HKEY','REPORT_DATE','POSITION_ROW_TYPE']
) }}

{% set position_diff_fields_close = abc_bank_position_diff_fields(
    prefix='curr.'
    , report_date_expr="to_varchar((select max(report_date) from stg_input), 'YYYY-MM-DD')"
    , quantity_expression='0'
    , cost_base_expression='0'
    , position_value_expression='0'
) %}

with

stg_input as (
    select 
        stg.* exclude (report_date, quantity, cost_base, position_value, load_ts_utc)
        , report_date
        , quantity
        , cost_base
        , position_value
        , load_ts_utc
        , 'OPEN' AS position_row_type
    from {{ ref('STG_ABC_BANK_POSITION') }} as stg
)

{% if is_incremental() %}

, current_from_history as (
    {{ current_from_history(
        history_rel = this
        , key_column = 'POSITION_HKEY'
        , history_filter_expr = "position_row_type = 'OPEN'"
    ) }}
)

, new_rows as (
    select stg.* 
    from stg_input as stg
    left join current_from_history curr
      on stg.position_hdiff = curr.position_hdiff
    where curr.position_hdiff is null
)

, closed_positions as (
    select 
        curr.* exclude (report_date, quantity, cost_base, position_value, load_ts_utc, position_row_type, position_hdiff)
        , (select max(report_date) from stg_input) as report_date
        , 0 as quantity
        , 0 as cost_base
        , 0 as position_value
        , '{{ run_started_at }}'::timestamp_ntz as load_ts_utc
        , 'CLOSE_SYNTHETIC' as position_row_type
        , {{ dbt_utils.generate_surrogate_key(position_diff_fields_close) }} as position_hdiff
    from current_from_history curr
    left join stg_input stg
        on stg.position_hkey = curr.position_hkey
    where stg.position_hkey is null
)

, changes_to_store as (
    select
        position_hkey
        , position_hdiff
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
    from new_rows

    union all

    select
        position_hkey
        , position_hdiff
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
    from closed_positions
)

{%- else %} -- full refresh or target table doesn't exist

, changes_to_store as (
    select * 
    from stg_input
)

{%- endif %}

, changes_dedup as (
  select *
  from changes_to_store
  qualify row_number() over (
           partition by
               position_hkey,
               report_date,
               position_row_type
           order by
               load_ts_utc desc,
               position_hdiff desc
         ) = 1
)


select * from changes_dedup