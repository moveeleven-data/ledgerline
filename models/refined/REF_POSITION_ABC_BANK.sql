WITH
current_from_snapshot as (
    SELECT *
    FROM {{ ref('SNSH_ABC_BANK_POSITION') }}
    WHERE DBT_VALID_TO is null
)
SELECT
    *,
    POSITION_VALUE - COST_BASE as UNREALIZED_PROFIT,
    ROUND(
       (POSITION_VALUE - COST_BASE) / NULLIF(COST_BASE, 0),
       5
    ) as UNREALIZED_PROFIT_PCT
FROM current_from_snapshot