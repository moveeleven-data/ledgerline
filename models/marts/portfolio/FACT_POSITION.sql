{{ config(materialized='table') }}

WITH

normalized AS (
    SELECT
        *,
        COALESCE(account_code,  '-1') AS account_code_nk,
        COALESCE(security_code, '-1') AS security_code_nk,
        COALESCE(exchange_code, '-1') AS exchange_code_nk,
        COALESCE(currency_code, '-1') AS currency_code_nk

    FROM {{ ref('REF_POSITION_ABC_BANK') }}
),

joined AS (
    SELECT
        n.report_date,
        n.quantity,
        n.cost_base,
        n.position_value,
        n.unrealized_profit,
        n.unrealized_profit_pct,
        n.record_source,
        n.load_ts_utc,

        da.account_key,
        ds.security_hkey  AS security_key,
        de.exchange_hkey  AS exchange_key,
        dc.currency_hkey  AS currency_key
    FROM normalized n
    JOIN {{ ref('DIM_ACCOUNT')  }} da ON n.account_code_nk  = da.account_code
    JOIN {{ ref('DIM_SECURITY') }} ds ON n.security_code_nk = ds.security_code
    JOIN {{ ref('DIM_EXCHANGE') }} de ON n.exchange_code_nk = de.exchange_code
    JOIN {{ ref('DIM_CURRENCY') }} dc ON n.currency_code_nk = dc.currency_code
)

SELECT *
FROM joined
