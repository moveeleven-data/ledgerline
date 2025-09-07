{{ config(materialized='ephemeral') }}

WITH

src_data AS (
    SELECT
        country_code_2_letter   AS COUNTRY_CODE2,
        country_code_3_letter   AS COUNTRY_CODE3,
        country_name            AS COUNTRY_NAME,
        region                  AS REGION,
        sub_region              AS SUB_REGION,
        country_code_numeric    AS COUNTRY_NUM_CODE,
        iso_3166_2              AS ISO_3166_2,
        region_code             AS REGION_CODE,
        sub_region_code         AS SUB_REGION_CODE,
        LOAD_TS                 AS LOAD_TS_UTC,
        'SEED.country_ISO_3166' AS RECORD_SOURCE

    FROM {{ source('seeds', 'country_ISO_3166') }}
),

default_record AS (
    SELECT
        '-1'                           AS COUNTRY_CODE2,
        '-1'                           AS COUNTRY_CODE3,
        'Missing'                      AS COUNTRY_NAME,
        NULL                           AS REGION,
        NULL                           AS SUB_REGION,
        NULL                           AS COUNTRY_NUM_CODE,
        NULL                           AS ISO_3166_2,
        NULL                           AS REGION_CODE,
        NULL                           AS SUB_REGION_CODE,
        TO_TIMESTAMP_NTZ('2020-01-01') AS LOAD_TS_UTC,
        'Missing'                      AS RECORD_SOURCE
),

with_default_record AS (
    SELECT * FROM src_data
    UNION ALL
    SELECT * FROM default_record
),

hashed AS (
    SELECT
        COUNTRY_CODE2 AS COUNTRY_HKEY,
        CONCAT_WS('|', COUNTRY_CODE2, COUNTRY_CODE3,
                       COUNTRY_NAME, REGION, SUB_REGION
                 ) AS COUNTRY_HDIFF,
        *
    FROM with_default_record
)

SELECT * FROM hashed;
