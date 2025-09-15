with dim_security as (

    {{ self_completing_dimension(
        dim_rel = ref('REF_SECURITY_INFO_ABC_BANK'),
        dim_key_column = 'SECURITY_CODE',
        dim_default_key_value = '-1',
        rel_columns_to_exclude = ['SECURITY_HKEY','SECURITY_HDIFF'],
        fact_defs = [ {'model': 'REF_POSITION_ABC_BANK', 'key': 'SECURITY_CODE'} ]
    ) }}

)

select
    security_code   as security_code,
    security_name   as security_name,
    sector_name     as sector_name,
    industry_name   as industry_name,
    country_code    as country_code,
    exchange_code   as exchange_code
from dim_security