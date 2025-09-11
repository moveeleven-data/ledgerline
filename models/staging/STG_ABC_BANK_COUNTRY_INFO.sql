{{ config(materialized='ephemeral') }}

with

src_data as (
    select
        country_code_2_letter        as COUNTRY_CODE2,
        country_code_3_letter        as COUNTRY_CODE3,
        country_name                 as COUNTRY_NAME,
        region                       as REGION,
        sub_region                   as SUB_REGION,
        country_code_numeric         as COUNTRY_NUM_CODE,
        iso_3166_2                   as ISO_3166_2,
        region_code                  as REGION_CODE,
        sub_region_code              as SUB_REGION_CODE,
        LOAD_TS                      as LOAD_TS,

        'SEED.ABC_BANK_COUNTRY_INFO' as RECORD_SOURCE

    from {{ source('seeds', 'ABC_Bank_COUNTRY_INFO') }}
),

default_record as (
    select
        '-1'                           as COUNTRY_CODE2,
        '-1'                           as COUNTRY_CODE3,
        'Missing'                      as COUNTRY_NAME,
        NULL                           as REGION,
        NULL                           as SUB_REGION,
        NULL                           as COUNTRY_NUM_CODE,
        NULL                           as ISO_3166_2,
        NULL                           as REGION_CODE,
        NULL                           as SUB_REGION_CODE,
        TO_TIMESTAMP_NTZ('2020-01-01') as LOAD_TS_UTC,
        'System.DefaultKey'            as RECORD_SOURCE
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
            'sub_region'
        ]) }} as country_hdiff,

        * exclude load_ts,
        load_ts as load_ts_utc
        
    from with_default_record
)

select * from hashed