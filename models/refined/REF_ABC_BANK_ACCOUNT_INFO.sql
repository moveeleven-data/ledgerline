WITH
current_rows as (
    SELECT * EXCLUDE (DBT_SCD_ID, DBT_UPDATED_AT,
                      DBT_VALID_FROM, DBT_VALID_TO)
    FROM {{ ref('SNSH_ABC_BANK_ACCOUNT_INFO') }}
    WHERE dbt_valid_to is null
)
SELECT *
FROM current_rows
