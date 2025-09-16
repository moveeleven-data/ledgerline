-- We use `check` strategy because we do not trust the REPORT_DATE
-- column to work as a good timestamp for the timestamp strategy.
{% snapshot SNSH_ABC_BANK_POSITION %}
{{
    config(
        enabled=false, 
        unique_key= 'POSITION_HKEY',
        strategy='check',
        check_cols=['POSITION_HDIFF'],
        invalidate_hard_deletes=True,
    )
}}
SELECT * FROM {{ ref('STG_ABC_BANK_POSITION') }}
{% endsnapshot %}