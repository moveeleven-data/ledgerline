{{ config(materialized='ephemeral') }}

WITH

src_data as (
    SELECT
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

    FROM {{ source('seeds', 'ABC_Bank_COUNTRY_INFO') }}
),

default_record as (
    SELECT
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
        'Missing'                      as RECORD_SOURCE
),

with_default_record as (
    SELECT * FROM src_data
    UNION ALL
    SELECT * FROM default_record
),

hashed as (
    SELECT
        COUNTRY_CODE2 as COUNTRY_HKEY,
        CONCAT_WS('|', COUNTRY_CODE2, COUNTRY_CODE3,
                       COUNTRY_NAME, REGION, SUB_REGION
                 )  as COUNTRY_HDIFF,

        * EXCLUDE LOAD_TS,
        LOAD_TS as LOAD_TS_UTC
    FROM with_default_record
)

SELECT * FROM hashed
