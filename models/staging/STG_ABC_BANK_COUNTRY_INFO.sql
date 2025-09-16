{{ config(materialized='ephemeral') }}

with src_data as (
    select
        country_code_2_letter    as country_code2,
        country_code_3_letter    as country_code3,
        country_name             as country_name,
        region                   as region,
        sub_region               as sub_region,
        country_code_numeric     as country_num_code,
        iso_3166_2               as iso_3166_2,
        region_code              as region_code,
        sub_region_code          as sub_region_code,
        load_ts                  as load_ts,

        'SEED.abc_bank_country_info' as record_source
    from {{ ref('abc_bank_country_info') }}
),

default_record as (
    select
        '-1'                            as country_code2,
        '-1'                            as country_code3,
        'Missing'                       as country_name,
        null                            as region,
        null                            as sub_region,
        null                            as country_num_code,
        null                            as iso_3166_2,
        null                            as region_code,
        null                            as sub_region_code,
        to_timestamp_ntz('2020-01-01')  as load_ts,
        'System.DefaultKey'             as record_source
),

with_default_record as (
    select * from src_data
    union all
    select * from default_record
),

hashed as (
    select
        {{ dbt_utils.generate_surrogate_key(['country_code2']) }} as country_hkey,
        {{ dbt_utils.generate_surrogate_key([
            'country_code2',
            'country_code3',
            'country_name',
            'region',
            'sub_region']
        ) }} as country_hdiff,

        * exclude (load_ts),

        load_ts as load_ts_utc
    from with_default_record
)

select * from hashed