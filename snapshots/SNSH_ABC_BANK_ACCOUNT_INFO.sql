{{ config(enabled=false) }}

{% snapshot SNSH_ABC_BANK_ACCOUNT_INFO %}
{{
    config(
        unique_key='ACCOUNT_HKEY',
        strategy='check',
        check_cols=['ACCOUNT_HDIFF']
    )
}}
SELECT * FROM {{ ref('STG_ABC_BANK_ACCOUNT_INFO') }}
{% endsnapshot %}