{% snapshot SNSH_ABC_BANK_SECURITY_INFO %}
{{
    config(
        enabled=false, 
        unique_key='SECURITY_HKEY',
        strategy='check',
        check_cols=['SECURITY_HDIFF']
    )
}}
SELECT * FROM {{ ref('STG_ABC_BANK_SECURITY_INFO') }}
{% endsnapshot %}