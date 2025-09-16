{% snapshot SNSH_ABC_BANK_COUNTRY_INFO %}
{{
    config(
        enabled=false, 
        unique_key='COUNTRY_HKEY',
        strategy='check',
        check_cols=['COUNTRY_HDIFF']
    )
}}
SELECT * FROM {{ ref('STG_ABC_BANK_COUNTRY_INFO') }}
{% endsnapshot %}