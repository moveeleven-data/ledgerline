{% snapshot SNSH_CURRENCY %}
{{
    config(
        unique_key='CURRENCY_HKEY',
        strategy='check',
        check_cols=['CURRENCY_HDIFF']
    )
}}
select * from {{ ref('STG_ABC_BANK_CURRENCY') }}
{% endsnapshot %}