{% snapshot SNSH_ABC_BANK_EXCHANGE_INFO %}
{{
    config(
        enabled=false, 
        unique_key='EXCHANGE_HKEY',
        strategy='check',
        check_cols=['EXCHANGE_HDIFF']
    )
}}
SELECT * FROM {{ ref('STG_ABC_BANK_EXCHANGE_INFO') }}
{% endsnapshot %}